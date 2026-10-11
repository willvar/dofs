package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/willvar/dofs"
	"github.com/willvar/dofs/config"
	"github.com/willvar/dofs/service"
)

var (
	Version = "dev"
	Commit  = "unknown"
)

type Options struct {
	Service service.Options
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer, options Options) error {
	if len(args) == 0 {
		printUsage(stderr)
		return errors.New("a command is required")
	}
	switch args[0] {
	case "init":
		return runInit(ctx, args[1:], stdout, stderr, options)
	case "namespace":
		return runNamespace(ctx, args[1:], stdout, stderr, options)
	case "mount":
		return runMount(ctx, args[1:], stdout, stderr, options)
	case "check":
		return runCheck(ctx, args[1:], stdout, stderr, options)
	case "version":
		_, err := fmt.Fprintf(stdout, "dofsd %s (%s)\n", Version, Commit)
		return err
	case "help", "-h", "--help":
		printUsage(stdout)
		return nil
	default:
		printUsage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runInit(ctx context.Context, args []string, stdout, stderr io.Writer, options Options) error {
	flags := newFlags("init", stderr)
	configPath := flags.String("config", "dofs.yaml", "configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected init argument %q", flags.Arg(0))
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	metadata, err := service.OpenMetadata(ctx, cfg, options.Service)
	if err != nil {
		return err
	}
	defer metadata.Close()
	_, err = fmt.Fprintf(stdout, "metadata ready (%s)\n", cfg.Metadata.Driver)
	return err
}

func runNamespace(ctx context.Context, args []string, stdout, stderr io.Writer, options Options) error {
	if len(args) == 0 {
		return errors.New("namespace requires create, list, or show")
	}
	switch args[0] {
	case "create":
		return runNamespaceCreate(ctx, args[1:], stdout, stderr, options)
	case "list":
		return runNamespaceList(ctx, args[1:], stdout, stderr, options)
	case "show":
		return runNamespaceShow(ctx, args[1:], stdout, stderr, options)
	case "delete":
		return runNamespaceDelete(ctx, args[1:], stdout, stderr, options)
	default:
		return fmt.Errorf("unknown namespace command %q", args[0])
	}
}

func runNamespaceDelete(ctx context.Context, args []string, stdout, stderr io.Writer, options Options) error {
	flags := newFlags("namespace delete", stderr)
	configPath := flags.String("config", "dofs.yaml", "configuration file")
	id := flags.String("id", "", "namespace ID")
	yes := flags.Bool("yes", false, "confirm permanent metadata and encrypted-object deletion")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected namespace delete argument %q", flags.Arg(0))
	}
	namespaceID := strings.TrimSpace(*id)
	if err := dofs.ValidateNamespaceID(namespaceID); err != nil {
		return err
	}
	if !*yes {
		return errors.New("namespace delete is permanent; pass --yes to confirm")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	runtime, err := service.OpenRuntime(ctx, cfg, options.Service)
	if err != nil {
		return err
	}
	defer runtime.Close()
	if err := dofs.DeleteNamespace(ctx, runtime.Metadata, runtime.Objects, namespaceID); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "deleted namespace %s\n", namespaceID)
	return err
}

func runNamespaceCreate(ctx context.Context, args []string, stdout, stderr io.Writer, options Options) error {
	flags := newFlags("namespace create", stderr)
	configPath := flags.String("config", "dofs.yaml", "configuration file")
	id := flags.String("id", "", "immutable namespace ID")
	label := flags.String("label", "", "human-readable label")
	quotaValue := flags.String("quota", "0", "plaintext quota, for example 100GiB; 0 is unlimited")
	jsonOutput := flags.Bool("json", false, "write JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected namespace create argument %q", flags.Arg(0))
	}
	if err := dofs.ValidateNamespaceID(strings.TrimSpace(*id)); err != nil {
		return err
	}
	quota, err := parseSize(*quotaValue)
	if err != nil {
		return fmt.Errorf("invalid namespace quota: %w", err)
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	metadata, err := service.OpenMetadata(ctx, cfg, options.Service)
	if err != nil {
		return err
	}
	defer metadata.Close()
	keys, err := service.OpenKeyProvider(cfg)
	if err != nil {
		return err
	}
	defer keys.Close()
	wrappedKey, err := keys.WrapNewNamespaceKey()
	if err != nil {
		return err
	}
	namespace, err := metadata.CreateNamespace(ctx, dofs.Namespace{
		ID: strings.TrimSpace(*id), Label: strings.TrimSpace(*label),
		WrappedKEK: wrappedKey, QuotaBytes: quota,
	})
	dofs.Clear(wrappedKey)
	if err != nil {
		return err
	}
	return printNamespace(stdout, namespace, *jsonOutput)
}

func runNamespaceList(ctx context.Context, args []string, stdout, stderr io.Writer, options Options) error {
	flags := newFlags("namespace list", stderr)
	configPath := flags.String("config", "dofs.yaml", "configuration file")
	jsonOutput := flags.Bool("json", false, "write JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected namespace list argument %q", flags.Arg(0))
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	metadata, err := service.OpenMetadata(ctx, cfg, options.Service)
	if err != nil {
		return err
	}
	defer metadata.Close()
	namespaces, err := metadata.ListNamespaces(ctx)
	if err != nil {
		return err
	}
	views := make([]namespaceView, 0, len(namespaces))
	for _, namespace := range namespaces {
		views = append(views, viewNamespace(namespace))
	}
	if *jsonOutput {
		return writeJSON(stdout, views)
	}
	for _, view := range views {
		if _, err := fmt.Fprintf(stdout, "%s\t%d\t%d\t%s\n", view.ID, view.UsedBytes, view.QuotaBytes, view.Label); err != nil {
			return err
		}
	}
	return nil
}

func runNamespaceShow(ctx context.Context, args []string, stdout, stderr io.Writer, options Options) error {
	flags := newFlags("namespace show", stderr)
	configPath := flags.String("config", "dofs.yaml", "configuration file")
	id := flags.String("id", "", "namespace ID")
	jsonOutput := flags.Bool("json", false, "write JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected namespace show argument %q", flags.Arg(0))
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	metadata, err := service.OpenMetadata(ctx, cfg, options.Service)
	if err != nil {
		return err
	}
	defer metadata.Close()
	namespace, err := metadata.GetNamespace(ctx, strings.TrimSpace(*id))
	if err != nil {
		return err
	}
	return printNamespace(stdout, namespace, *jsonOutput)
}

func runMount(ctx context.Context, args []string, stdout, stderr io.Writer, options Options) error {
	flags := newFlags("mount", stderr)
	configPath := flags.String("config", "dofs.yaml", "configuration file")
	namespaceID := flags.String("namespace", "", "namespace ID")
	mountpoint := flags.String("mountpoint", "", "mountpoint inside mount.root")
	readOnly := flags.Bool("read-only", false, "override config and mount read-only")
	writable := flags.Bool("writable", false, "override config and mount writable")
	debug := flags.Bool("debug", false, "enable FUSE debug logging")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected mount argument %q", flags.Arg(0))
	}
	if *readOnly && *writable {
		return errors.New("--read-only and --writable are mutually exclusive")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	mountWritable := cfg.Mount.Writable
	if *readOnly {
		mountWritable = false
	}
	if *writable {
		mountWritable = true
	}
	runtime, err := service.OpenRuntime(ctx, cfg, options.Service)
	if err != nil {
		return err
	}
	defer runtime.Close()
	if _, err := fmt.Fprintf(stdout, "mounting namespace %s\n", strings.TrimSpace(*namespaceID)); err != nil {
		return err
	}
	return service.MountNamespace(ctx, cfg, runtime, service.MountRequest{
		NamespaceID: strings.TrimSpace(*namespaceID), Mountpoint: strings.TrimSpace(*mountpoint),
		Writable: mountWritable, Debug: cfg.Mount.Debug || *debug,
	})
}

func runCheck(ctx context.Context, args []string, stdout, stderr io.Writer, options Options) error {
	flags := newFlags("check", stderr)
	configPath := flags.String("config", "dofs.yaml", "configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected check argument %q", flags.Arg(0))
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	runtime, err := service.OpenRuntime(ctx, cfg, options.Service)
	if err != nil {
		return err
	}
	defer runtime.Close()
	if err := runtime.Check(ctx); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "DOFS check passed")
	return err
}

type namespaceView struct {
	ID                  string    `json:"id"`
	Label               string    `json:"label"`
	QuotaBytes          int64     `json:"quota_bytes"`
	UsedBytes           int64     `json:"used_bytes"`
	HistoryLimit        int64     `json:"history_limit"`
	HistoryBytes        int64     `json:"history_bytes"`
	PendingReclaimBytes int64     `json:"pending_reclaim_bytes"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func viewNamespace(namespace dofs.Namespace) namespaceView {
	return namespaceView{
		ID: namespace.ID, Label: namespace.Label, QuotaBytes: namespace.QuotaBytes,
		UsedBytes: namespace.UsedBytes, HistoryLimit: namespace.HistoryLimit,
		HistoryBytes: namespace.HistoryBytes, PendingReclaimBytes: namespace.PendingReclaimBytes,
		CreatedAt: namespace.CreatedAt, UpdatedAt: namespace.UpdatedAt,
	}
}

func printNamespace(writer io.Writer, namespace dofs.Namespace, jsonOutput bool) error {
	view := viewNamespace(namespace)
	if jsonOutput {
		return writeJSON(writer, view)
	}
	_, err := fmt.Fprintf(writer, "%s\t%d\t%d\t%s\n", view.ID, view.UsedBytes, view.QuotaBytes, view.Label)
	return err
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func newFlags(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet("dofsd "+name, flag.ContinueOnError)
	flags.SetOutput(output)
	return flags
}

func parseSize(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "unlimited") {
		return 0, nil
	}
	upper := strings.ToUpper(value)
	multipliers := []struct {
		suffix     string
		multiplier int64
	}{
		{"TIB", 1 << 40}, {"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
		{"TB", 1_000_000_000_000}, {"GB", 1_000_000_000}, {"MB", 1_000_000}, {"KB", 1_000}, {"B", 1},
	}
	multiplier := int64(1)
	for _, candidate := range multipliers {
		if strings.HasSuffix(upper, candidate.suffix) {
			upper = strings.TrimSpace(strings.TrimSuffix(upper, candidate.suffix))
			multiplier = candidate.multiplier
			break
		}
	}
	number, err := strconv.ParseInt(upper, 10, 64)
	if err != nil || number < 0 {
		return 0, errors.New("size must be a non-negative integer with an optional B/KB/KiB/MB/MiB/GB/GiB/TB/TiB suffix")
	}
	if number > math.MaxInt64/multiplier {
		return 0, errors.New("size exceeds int64")
	}
	return number * multiplier, nil
}

func printUsage(writer io.Writer) {
	fmt.Fprintln(writer, "usage: dofsd <command> [options]")
	fmt.Fprintln(writer, "commands: init, namespace create|list|show|delete, mount, check, version")
}
