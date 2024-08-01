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
	_, err := storeService.redisClient.Exists(ctx, shortUrl).Result()
	if err != nil {
		panic(err)
	}
	check := DatabaseSqlCheck(shortUrl)
	if check {
		count := GetCount(shortUrl)
		if count > 10 {
			DeleteDatabaseSql(shortUrl)
			res.OriginalUrl = originalUrl
			res.Count = 0
			jsonData, err := json.Marshal(res)
			if err != nil {
				panic(err)
			}
			err = storeService.redisClient.Set(ctx, shortUrl, jsonData, CacheDuration).Err()
			if err != nil {
				panic(fmt.Sprintf("Failed saving key url | Error: %v - shortUrl: %s - originalUrl: %s\n", err, shortUrl, originalUrl))
			}
			fmt.Printf("Saved shortUrl: %s - originalUrl: %s\n", shortUrl, originalUrl)
		}
	} else {
		AddDatabaseSql(shortUrl, originalUrl, 1)
	}
}

func RetrieveInitialUrl(shortUrl string) string {
	var res UrlCount
	var url string
	re, err := storeService.redisClient.Exists(ctx, shortUrl).Result()
	if err != nil {
		fmt.Sprintf("ERROR: %v - shortUrl: %s\n", err, shortUrl)
	}
	if re != 0 {
		result, err := storeService.redisClient.Get(ctx, shortUrl).Bytes()
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(result, &res)
		if err != nil {
			panic(err)
		}
		res.Count = res.Count + 1
		url = res.OriginalUrl
		fmt.Println("\n\n\n\n", "redis: ", res.OriginalUrl, "\n", res.Count, "\n\n\n\n")
		jsonData, err := json.Marshal(res)
		if err != nil {
			panic(err)
		}
		err = storeService.redisClient.Set(ctx, shortUrl, jsonData, CacheDuration).Err()
		if err != nil {
			panic(fmt.Sprintf("Failed RetrieveInitialUrl url | Error: %v - shortUrl: %s\n", res, shortUrl))
		}
		return url
	} else {
		originalUrl, count := GetCountDatabaseSql(shortUrl)
		res.OriginalUrl = originalUrl
		res.Count = count + 1
		fmt.Println("\n\n\n\n", "Postgres: ", res.OriginalUrl, "\n", res.Count, "\n\n\n\n")
		DeleteDatabaseSql(shortUrl)
		AddDatabaseSql(shortUrl, originalUrl, count+1)
		return originalUrl
	}
	return res.OriginalUrl
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

func DeleteDatabaseSql(shortUrl string) {
	const connStr = "postgres://postgres:2412@localhost:5432/MyBD?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Удаление данных из таблицы
	_, err = db.Exec(`
		DELETE FROM shortened_urls
		WHERE short_url = $1
	`, shortUrl)
	if err != nil {
		panic(err)
	}
}
func DatabaseSqlCheck(shortUrl string) bool {
	const connStr = "postgres://postgres:2412@localhost:5432/MyBD?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Проверка существования значения short_url
	var exists bool
	err = db.QueryRow(`
        SELECT count (
            FROM shortened_urls
            WHERE short_url = $1
        )
    `, shortUrl).Scan(&exists)
	if err != nil {
		panic(err)
	}
	if exists {
		fmt.Println("Значение shortUrl существует в базе данных postgres")
	} else {
		fmt.Println("Значение shortUrl не существует в базе данных postgres")
	}
	return exists
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
	if count > 10 {
		SaveUrlRedis(shortUrl, originalUrl, count)
		DeleteDatabaseSql(shortUrl)
	}
	return originalUrl, count
}

func SaveUrlRedis(shortUrl string, originalUrl string, count int) {
	var url UrlCount
	url.OriginalUrl = originalUrl
	url.Count = count
	err := storeService.redisClient.Set(ctx, shortUrl, url, CacheDuration).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed SaveUrlRedis url | Error: %v - shortUrl: %s\n", err, shortUrl))
	}
}
func GetCount(shortUrl string) int {
	const connStr = "postgres://postgres:2412@localhost:5432/MyBD?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// Получение данных по значению short_url
	var count int
	err = db.QueryRow(`
        SELECT  count
        FROM shortened_urls
        WHERE short_url = $1
    `, shortUrl).Scan(&count)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			fmt.Println("Значение short_url не существует в базе данных")
		} else {
			panic(err)
		}
	} else {
		fmt.Println("count:", count)
	}
	return count
}
