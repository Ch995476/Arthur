package userbot

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/yanakipre/bot/app/telegramsearch/internal/pkg/staticconfig"
	"github.com/yanakipre/bot/internal/logger"
	"github.com/yanakipre/bot/internal/scheduletooling"
)

func NewSchedulerCmd(cfg *staticconfig.Config) *cobra.Command {
	return &cobra.Command{
		Use:   "scheduler",
		Short: "Start scheduler [read] [thread] [embed]",
		Long:  "Starts a background scheduler",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			logger.Info(ctx, "Starting telegram chat reader scheduler")

			var runRead, runDump, runEmbeddings bool
			if len(args) == 0 {
				runRead, runDump, runEmbeddings = true, true, true
			} else {
				for _, a := range args {
					switch a {
					case "read":
						runRead = true
					case "thread":
						runDump = true
					case "embed":
						runEmbeddings = true
					default:
						return fmt.Errorf("unknown job argument: %s", a)
					}
				}
			}
			fmt.Println("111111111111111111111")
			fmt.Println(runRead, runDump, runEmbeddings)
			fmt.Println("111111111111111111111")
			scheduler := scheduletooling.NewScheduler(1 * time.Second)

			if runRead {
				job := NewChatReaderJob(cfg)
				if err := scheduler.Add(ctx, job); err != nil {
					return err
				}
			}

			if runDump {
				historyDumpJob := NewChatHistoryDumpJob(cfg)
				if err := scheduler.Add(ctx, historyDumpJob); err != nil {
					return err
				}
			}

			if runEmbeddings {
				embeddingsJob := NewEmbeddingsGenerationJob(cfg)
				if err := scheduler.Add(ctx, embeddingsJob); err != nil {
					return err
				}
			}

			scheduler.Start(ctx)
			logger.Info(ctx, "Scheduler started with chat reader (every 15 seconds), history dump (every 30 minutes) and embeddings generation (every 30 minutes) jobs")

			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

			select {
			case <-ctx.Done():
				logger.Info(ctx, "Context cancelled, shutting down scheduler")
			case sig := <-sigChan:
				logger.Info(ctx, "Received signal, shutting down scheduler", zap.String("signal", sig.String()))
			}

			logger.Info(ctx, "Stopping scheduler...")
			scheduler.Stop()
			scheduler.Wait(ctx)
			logger.Info(ctx, "Scheduler stopped")

			return nil
		},
	}
}
