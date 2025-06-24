package scrapping

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"scrapping_service/internal/database"
	"scrapping_service/internal/kafka"
	"scrapping_service/internal/scrapping/external"
	"scrapping_service/internal/scrapping/graph"
	"scrapping_service/internal/scrapping/graph/server"
	"scrapping_service/internal/scrapping/migrations"
	"scrapping_service/internal/scrapping/repository"
	"scrapping_service/pkg/middlewares"
	"scrapping_service/pkg/utils"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	md "github.com/JohannesKaufmann/html-to-markdown"
	"github.com/PuerkitoBio/goquery"
	"github.com/go-co-op/gocron"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
)

var (
	articleMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name: "articles",
		Help: "The total number of successful article parsings",
	})
)

type Conf struct {
	Host      string `yaml:"host"`
	ScrapCron int    `yaml:"scrapCron"`
	CheckAuth bool   `yaml:"checkAuth"`
}

type Service struct {
	utils.Conv

	ctx   context.Context
	xConf sync.RWMutex
	conf  *Conf

	server *http.Server
	client *http.Client
	kafka  kafka.Kafka

	repo *repository.Repository

	cron *gocron.Scheduler

	// поля для скрапинга
	converter *md.Converter
}

func NewService(ctx context.Context, name, namespace string) *Service {
	return &Service{
		ctx:       ctx,
		Conv:      utils.NewConv(name, namespace),
		cron:      gocron.NewScheduler(time.UTC),
		converter: md.NewConverter("", true, nil),
	}
}

func (s *Service) Join(kafka kafka.Kafka) {
	s.kafka = kafka
}

func (s *Service) Configure(conf *Conf, confDb *database.Conf) {
	log.Info().Str("module", s.Name).Msg("conf: configure begin")

	s.setConf(conf)

	s.Load.Do(func() {

		s.converter.Before(func(item *goquery.Selection) {
			item.Find("img").Each(func(i int, item *goquery.Selection) {
				src, ok := item.Attr("src")
				if !ok {
					return
				}
				item.SetAttr("src", item.AttrOr("data-src", src))
			})
		})

		// подключаемся к БД
		db := database.NewDatabase(s.ctx, "database", "scrapping")
		db.Configure(confDb)

		// запускаем миграции
		migrations.MigrateUp(db.DBX.DB)

		s.repo = repository.NewRepository(db.DBX)

		s.client = &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if strings.Contains(req.URL.String(), "articles") {
				return nil
			}
			return errors.New("redirected")
		}}

		s.RunWorker(s.start, "start", 1)

		_, err := s.cron.Every(conf.ScrapCron).Minutes().Do(s.scrap)
		if err != nil {
			log.Error().Str("module", s.Name).Msgf("start cron for scrapping: %v", err)
		}

		s.cron.StartAsync()
	})

	log.Info().Str("module", s.Name).Msg("conf: configure end")
}

func (s *Service) setConf(conf *Conf) {
	s.xConf.Lock()
	s.conf = conf
	s.xConf.Unlock()
}

func (s *Service) getConf() *Conf {
	s.xConf.RLock()
	defer s.xConf.RUnlock()
	return s.conf
}

func (s *Service) start() {
	defer log.Info().Str("module", s.Name).Msg("start worker closed")

	graphConf := server.Config{Resolvers: &graph.Resolver{Scrapping: s}}

	srv := handler.NewDefaultServer(server.NewExecutableSchema(graphConf))

	r := chi.NewRouter()

	// todo для локальных тестов с фронтендом, для прода убрать
	r.Use(middlewares.Logger(s.Name), cors.AllowAll().Handler)

	r.Get("/api/v1/scrapping/health", checkHealth)

	r.Get("/api/v1/scrapping/article/{id}", s.GetArticle)

	r.Handle("/api/v1/scrapping/graph/query", middlewares.Auth(srv, s.getConf().CheckAuth))

	r.Handle("/api/v1/scrapping/graph/playground", playground.AltairHandler("GraphQL Scrapping Playground", "/api/v1/scrapping/graph/query"))

	r.Handle("/metrics", promhttp.Handler())

	for {

		conf := s.getConf()
		log.Info().Str("module", s.Name).Msgf("scrapper http server starting on %s", conf.Host)
		log.Info().Str("module", s.Name).Msgf("scrapper graphql starting on  http://localhost%s/scrapping/v1/graph/scrapping/playground", conf.Host)

		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// запускаем http сервер
		s.server = &http.Server{
			Addr:    s.getConf().Host,
			Handler: r,
		}

		if err := s.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Error().Str("module", s.Name).Msgf("scrapper server http error %v", err)
		} else {
			log.Info().Str("module", s.Name).Msgf("scrapper server http shudown %v", err)
			return
		}

		time.Sleep(time.Second)
	}
}

func (s *Service) scrap() {
	log.Info().Str("module", s.Name).Msg("scrap start")
	defer log.Info().Str("module", s.Name).Msg("scrap end")
	lastArticleSite, err := getLastArticle()
	if err != nil {
		log.Error().Str("module", s.Name).Msgf("GetLastArticle from site error: %v", err)
		return
	}

	lastArticle, err := s.repo.GetLastArticle(s.ctx)
	if err != nil {
		if repository.IsNotFoundError(err) {
			lastArticle = lastArticleSite - 50
		} else {
			log.Error().Str("module", s.Name).Msgf("GetLastArticle from repo error: %v", err)
			return
		}
	}
	if lastArticleSite < lastArticle {
		log.Warn().Str("module", s.Name).Msgf("strange article ids: in repo %v, in site %v", lastArticle, lastArticleSite)
		return
	}

	firstArticle, err := s.repo.GetFirstArticle(s.ctx)
	if err != nil {
		if repository.IsNotFoundError(err) {
			firstArticle = lastArticle - 1
		} else {
			log.Error().Str("module", s.Name).Msgf("GetFirstArticle from repo error: %v", err)
			return
		}
	}
	ids := utils.CreateRangeSlice([2]int64{firstArticle - 50, firstArticle - 1}, [2]int64{lastArticle + 1, lastArticleSite})
	var wg sync.WaitGroup
	result := make(chan *external.Article)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, i := range ids {
			// выходим из цикла
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			time.Sleep(50 * time.Millisecond)
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()
				article, err := s.getArticle(i)
				if err != nil {
					log.Info().Str("module", s.Name).Msgf("getArticle error: %v, url: %v", err, i)
					return
				}
				result <- article
			}(i)
		}
	}()

	go func() {
		wg.Wait()
		close(result)
	}()

	for article := range result {
		articleMetric.Inc()
		repoArticle, err := mapArticle(article)
		if err != nil {
			log.Error().Str("module", s.Name).Msgf("mapArticle error: %v", err)
			continue
		}
		err = s.repo.AddArticle(s.ctx, repoArticle)
		if err != nil {
			log.Error().Str("module", s.Name).Msgf("error in repo: %v", err)
			continue
		}
		s.kafka.SendAsyncMessage(json.RawMessage(fmt.Sprintf(`{"id":%v}`, article.Id)))
	}
}

func getLastArticle() (int64, error) {
	doc, err := goquery.NewDocument("https://habr.com/ru/articles/")
	if err != nil {
		return 0, err
	}

	var articleID int64
	var parseErr error
	found := false

	doc.Find("article[class=tm-articles-list__item]").First().Each(func(i int, item *goquery.Selection) {
		val, ok := item.Attr("id")
		if !ok {
			parseErr = errors.New("attribute 'id' not found")
			return
		}

		id, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			parseErr = err
			return
		}

		articleID = id
		found = true
	})

	if parseErr != nil {
		return 0, parseErr
	}

	if !found {
		return 0, errors.New("no article found")
	}

	return articleID, nil
}

func (s *Service) getArticle(id int64) (*external.Article, error) {
	url := fmt.Sprintf("https://habr.com/ru/articles/%v/", id)
	get, err := s.client.Get(url)
	if err != nil {
		return nil, err
	}
	if get.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("not received 200 status code: %v", get.StatusCode)
	}
	defer get.Body.Close()
	doc, err := goquery.NewDocumentFromReader(get.Body)
	if err != nil {
		return nil, err
	}
	var (
		name, text, complexity string
		readingTime            int64
		tags                   []string
	)
	doc.Find("div[xmlns='http://www.w3.org/1999/xhtml']").Each(func(i int, item *goquery.Selection) {
		text = s.converter.Convert(item)
	})

	doc.Find("h1[class='tm-title tm-title_h1']").Each(func(i int, item *goquery.Selection) {
		name = item.Text()
	})

	doc.Find("span[class=tm-article-complexity__label]").Each(func(i int, item *goquery.Selection) {
		complexity = item.Text()
	})

	doc.Find("span[class=tm-article-reading-time__label]").Each(func(i int, item *goquery.Selection) {
		arr := strings.Split(item.Text(), " ")
		readingTime, _ = strconv.ParseInt(arr[0], 10, 64)
	})

	doc.Find("a[class=tm-tags-list__link]").Each(func(i int, item *goquery.Selection) {
		tag := item.Text()
		if tag != "" {
			tags = append(tags, tag)
		}
	})

	return &external.Article{
		Id:          id,
		Name:        name,
		Text:        text,
		Complexity:  complexity,
		ReadingTime: readingTime,
		Tags:        tags,
	}, nil
}

func mapArticle(article *external.Article) (*repository.Article, error) {
	tags, err := json.Marshal(article.Tags)
	if err != nil {
		return nil, err
	}
	complexity := sql.NullString{String: article.Complexity}
	if complexity.String != "" {
		complexity.Valid = true
	}
	return &repository.Article{
		Id:          article.Id,
		Name:        article.Name,
		Text:        article.Text,
		Complexity:  complexity,
		ReadingTime: article.ReadingTime,
		Tags:        tags,
	}, nil
}

func (s *Service) GetArticle(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	idStr := chi.URLParam(r, "id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	dbArticle, err := s.repo.GetArticleById(r.Context(), id)
	if err != nil {
		if repository.IsNotFoundError(err) {
			http.NotFound(w, r)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		}
		return
	}
	var tags []string
	err = json.Unmarshal(dbArticle.Tags, &tags)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	article := external.Article{
		Id:          dbArticle.Id,
		Name:        dbArticle.Name,
		Text:        dbArticle.Text,
		Complexity:  dbArticle.Complexity.String,
		ReadingTime: dbArticle.ReadingTime,
		Tags:        tags,
	}
	response, err := json.Marshal(article)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(response)
}

func (s *Service) WaitTerminate() {
	log.Info().Str("module", s.Name).Msg("scrapper server term: begin")

	s.cron.Stop()

	ctx, cancel := context.WithTimeout(s.ctx, time.Second*10)

	defer func() {
		cancel()
	}()

	if err := s.server.Shutdown(ctx); err != nil {
		log.Err(err).Str("module", s.Name)
	}

	s.WaitWorker("start")

	log.Info().Str("module", s.Name).Msg("scrapper server term: begin")
}

func checkHealth(w http.ResponseWriter, r *http.Request) {
	// Простая проверка здоровья, отвечаем статусом 200
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
