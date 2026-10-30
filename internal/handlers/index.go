package handlers

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

func FaviconHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Print(1111111111111111)
	http.ServeFile(w, r, "favicon.ico")
}

func IndexHandler(w http.ResponseWriter, r *http.Request) {
	data, err := os.ReadFile("internal/html/index.html")
	if err != nil {
		log.Printf("Ошибка чтения файла: %v", err)
		http.Error(w, "Не удалось прочитать шаблон", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	dataHeader, _ := os.ReadFile("internal/html/_header.html")
	w.Write(dataHeader)
	w.Write(data)
	dataFooter, _ := os.ReadFile("internal/html/_footer.html")
	w.Write(dataFooter)

}
