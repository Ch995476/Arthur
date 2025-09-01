package postgres

import (
	"context"
	"encoding/json"

	"github.com/yanakipre/bot/app/telegramsearch/internal/pkg/client/storage/postgres/internal/dbmodels"
	models "github.com/yanakipre/bot/app/telegramsearch/internal/pkg/client/storage/storagemodels"

	"github.com/samber/lo"
	"github.com/yanakipre/bot/internal/sqltooling"
)

var queryFetchChatThreadToGenerateEmbedding = sqltooling.NewStmt(
	"FetchChatThreadToGenerateEmbedding",
	`
SELECT thread_id, chat_id, body FROM chatthreads WHERE most_recent_message_at > fresh_embeddings_at LIMIT 2000;
`,
	dbmodels.ChatThread{},
)

func (s *Storage) FetchChatThreadToGenerateEmbedding(ctx context.Context, req models.ReqFetchChatThreadToGenerateEmbedding) (models.RespFetchChatThreadToGenerateEmbedding, error) {
	rows := []dbmodels.ChatThread{}
	if err := s.db.SelectContext(ctx, &rows, queryFetchChatThreadToGenerateEmbedding.Query, map[string]any{}); err != nil { // map[string]any{} generate?
		return models.RespFetchChatThreadToGenerateEmbedding{}, err
	}
	return models.RespFetchChatThreadToGenerateEmbedding{
		Threads: lo.Map(rows, func(item dbmodels.ChatThread, _ int) models.ChatThreadToGenerateEmbedding {
			return models.ChatThreadToGenerateEmbedding{
				ChatID:   item.ChatID,
				ThreadID: item.ThreadID,
				Body:     item.Body,
			}
		}),
	}, nil
}

var queryCreateChatThread = sqltooling.NewStmt(
	"CreateChatThread",
	`
INSERT INTO chatthreads
	(chat_id, body, content_hash)
VALUES (
        $1, CAST($2 as JSONB), md5($2::text)
)
ON CONFLICT (chat_id, content_hash) DO NOTHING;
`,
	nil,
)

func (s *Storage) CreateChatThread(ctx context.Context, req models.ReqCreateChatThread) (models.RespCreateChatThread, error) {
	marshal, err := json.Marshal(req.Body)
	if err != nil {
		return models.RespCreateChatThread{}, err
	}

	_, err = s.db.ExecContext(ctx, queryCreateChatThread.Query, req.ChatID, marshal)
	if err != nil {
		return models.RespCreateChatThread{}, err
	}
	return models.RespCreateChatThread{}, nil
}
