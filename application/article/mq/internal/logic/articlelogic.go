package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"beyond/application/article/mq/internal/svc"
	"beyond/application/article/mq/internal/types"
	"beyond/application/user/rpc/user"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/zeromicro/go-zero/core/logx"
)

type ArticleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewArticleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ArticleLogic {
	return &ArticleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ArticleLogic) Consume(_, val string) error {
	logx.Infof("Consume msg val: %s", val)
	var msg *types.CanalArticleMsg
	err := json.Unmarshal([]byte(val), &msg)
	if err != nil {
		logx.Errorf("Consume val: %s error: %v", val, err)
		return err
	}

	return l.articleOperate(msg)
}

func (l *ArticleLogic) articleOperate(msg *types.CanalArticleMsg) error {
	fmt.Println("========articleOperate")
	if len(msg.Data) == 0 {
		return nil
	}

	var esData []*types.ArticleEsMsg
	for _, d := range msg.Data {
		status, _ := strconv.Atoi(d.Status)
		likNum, _ := strconv.ParseInt(d.LikeNum, 10, 64)
		articleId, _ := strconv.ParseInt(d.ID, 10, 64)
		authorId, _ := strconv.ParseInt(d.AuthorId, 10, 64)

		t, err := time.ParseInLocation("2006-01-02 15:04:05", d.PublishTime, time.Local)
		publishTimeKey := articlesKey(d.AuthorId, 0)
		likeNumKey := articlesKey(d.AuthorId, 1)

		switch status {
		case types.ArticleStatusVisible:
			b, _ := l.svcCtx.BizRedis.ExistsCtx(l.ctx, publishTimeKey)
			if b {
				_, err = l.svcCtx.BizRedis.ZaddCtx(l.ctx, publishTimeKey, t.Unix(), d.ID)
				if err != nil {
					l.Logger.Errorf("ZaddCtx key: %s req: %v error: %v", publishTimeKey, d, err)
				}
			}
			b, _ = l.svcCtx.BizRedis.ExistsCtx(l.ctx, likeNumKey)
			if b {
				_, err = l.svcCtx.BizRedis.ZaddCtx(l.ctx, likeNumKey, likNum, d.ID)
				if err != nil {
					l.Logger.Errorf("ZaddCtx key: %s req: %v error: %v", likeNumKey, d, err)
				}
			}
		case types.ArticleStatusUserDelete:
			_, err = l.svcCtx.BizRedis.ZremCtx(l.ctx, publishTimeKey, d.ID)
			if err != nil {
				l.Logger.Errorf("ZremCtx key: %s req: %v error: %v", publishTimeKey, d, err)
			}
			_, err = l.svcCtx.BizRedis.ZremCtx(l.ctx, likeNumKey, d.ID)
			if err != nil {
				l.Logger.Errorf("ZremCtx key: %s req: %v error: %v", likeNumKey, d, err)
			}
		}

		u, err := l.svcCtx.UserRPC.FindById(l.ctx, &user.FindByIdRequest{
			UserId: authorId,
		})
		if err != nil {
			l.Logger.Errorf("FindById userId: %d error: %v", authorId, err)
			return err
		}

		a := &types.ArticleEsMsg{
			ArticleId:   articleId,
			AuthorId:    authorId,
			AuthorName:  u.Username,
			Title:       d.Title,
			Content:     d.Content,
			Description: d.Description,
			Status:      status,
			LikeNum:     likNum,
		}
		fmt.Println("ArticleEsMsg", *a)

		esData = append(esData, a)
	}
	fmt.Println("========toES")
	fmt.Println("-----", len(esData), "------")

	err := l.BatchUpSertToEs(l.ctx, esData)
	if err != nil {
		fmt.Println("========toES ERR")
		l.Logger.Errorf("BatchUpSertToEs data: %v error: %v", esData, err)
	} else {
		fmt.Println("========toES OK")
	}

	return err
}

func (l *ArticleLogic) BatchUpSertToEs(ctx context.Context, data []*types.ArticleEsMsg) error {
	if len(data) == 0 {
		return nil
	}

	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        l.svcCtx.Es.Client,
		Index:         "article-index",
		NumWorkers:    3,                // The number of worker goroutines
		FlushBytes:    102400,           // The flush threshold in bytes
		FlushInterval: 30 * time.Second, // The periodic flush interval
		OnError: func(ctx context.Context, err error) {
			logx.Infof("BulkIndexerConfig Unexpected error: %s", err)
		},
	})
	if err != nil {
		return err
	}

	start := time.Now().UTC()
	for _, d := range data {
		v, err := json.Marshal(d)
		if err != nil {
			return err
		}

		payload := fmt.Sprintf(`{"doc":%s,"doc_as_upsert":true}`, string(v))
		err = bi.Add(ctx, esutil.BulkIndexerItem{
			Action:     "update",
			Index:      "article-index",
			DocumentID: fmt.Sprintf("%d", d.ArticleId),
			Body:       strings.NewReader(payload),
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem) {
				fmt.Println("OnSuccess")
			},
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error) {
				fmt.Println("OnFailure")
			},
		})
		if err != nil {
			return err
		}

		biStats := bi.Stats()
		// Report the results: number of indexed docs, number of errors, duration, indexing rate
		//
		log.Println(strings.Repeat("-", 80))
		dur := time.Since(start)
		if biStats.NumFailed > 0 {
			log.Printf("[YhkbElReceiver.BulkUpsertKnowledge]总数据[%d]行，其中失败[%d]， 耗时 %v (速度：%d docs/秒)\n", biStats.NumAdded, biStats.NumFailed,
				dur.Truncate(time.Millisecond), int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed)),
			)
		} else {
			log.Printf("[YhkbElReceiver.BulkUpsertKnowledge]处理数据[%d]行，耗时%v (速度：%d docs/秒)\n", biStats.NumFlushed, dur.Truncate(time.Millisecond),
				int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed)),
			)
		}
		log.Println(strings.Repeat("-", 80))
	}

	return bi.Close(ctx)
}

func articlesKey(uid string, sortType int32) string {
	return fmt.Sprintf("biz#articles#%s#%d", uid, sortType)
}
