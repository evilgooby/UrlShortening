package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"time"
)

var (
	storeService = &StorageService{}
	ctx          = context.Background()
)

const CacheDuration = 15 * time.Minute

type StorageService struct {
	redisClient *redis.Client
}
type UrlCount struct {
	OriginalUrl string `json:"originalUrl"`
	Count       int    `json:"count"`
}

func InitializeStore() *StorageService {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// Подключение к базе данных
	const connStr = "postgres://postgres:2412@localhost:5432/MyBD?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Выполнение инструкции SQL для создания
	pong, err := redisClient.Ping(ctx).Result()
	if err != nil {
		panic(fmt.Sprintf("Error init Redis: %v", err))
	}

	fmt.Printf("\nRedis started successfully: pong message = {%s}", pong)
	storeService.redisClient = redisClient
	return storeService
}

func SaveUrlMapping(shortUrl string, originalUrl string, userId string) {
	var res UrlCount
	AddDatabaseSql(shortUrl, originalUrl, 1)
	res.OriginalUrl = originalUrl
	res.Count = 1
	jsonData, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}
	err = storeService.redisClient.Set(ctx, shortUrl, jsonData, CacheDuration).Err()
	if err != nil {
		panic(err)
	}
}

func RetrieveInitialUrl(shortUrl string) string {
	var res UrlCount
	var url string
	re, err := storeService.redisClient.Exists(ctx, shortUrl).Result()
	if err != nil {
		fmt.Sprintf("ERROR: %v - shortUrl: %s\n", err, shortUrl)
	}
	if re == 0 {
		orUrl, count := GetCountDatabaseSql(shortUrl)
		res.OriginalUrl = orUrl
		res.Count = count
		url = orUrl
		fmt.Println("Postgresql")
		jsonData, err := json.Marshal(res)
		if err != nil {
			panic(err)
		}
		err = storeService.redisClient.Set(ctx, shortUrl, jsonData, CacheDuration).Err()
		if err != nil {
			panic(err)
		}
	} else {
		jsonData, err := storeService.redisClient.Get(ctx, shortUrl).Result()
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal([]byte(jsonData), &res)
		if err != nil {
			panic(err)
		}
		url = res.OriginalUrl
		fmt.Println("Redis")
	}
	return url
}

func AddDatabaseSql(shortUrl string, originalUrl string, count int) {
	const connStr = "postgres://postgres:2412@localhost:5432/MyBD?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Добавление данных в таблицу
	_, err = db.Exec(`	
		INSERT INTO shortened_urls (short_url, original_url, count)
		VALUES ($1, $2, $3)
	`, shortUrl, originalUrl, count)
	if err != nil {
		panic(err)
	}
}

func GetCountDatabaseSql(shortUrl string) (string, int) {
	const connStr = "postgres://postgres:2412@localhost:5432/MyBD?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Получение данных по значению short_url
	var originalUrl string
	var count int
	err = db.QueryRow(`
        SELECT original_url, count
        FROM shortened_urls
        WHERE short_url = $1
    `, shortUrl).Scan(&originalUrl, &count)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			fmt.Println("Значение short_url не существует в базе данных")
		} else {
			panic(err)
		}
	} else {
		fmt.Println("count:", count)
	}

	return originalUrl, count
}
