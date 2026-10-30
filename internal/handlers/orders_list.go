package handlers

import (
	"log"
	// "my_api/internal/api"
	"net/http"
	"os"
)

func OrdersListHandler(w http.ResponseWriter, r *http.Request) {
	data, err := os.ReadFile("internal/html/order_list.html")
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
