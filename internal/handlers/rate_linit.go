package handlers

import (
	"fmt"
	// "log"
	// "my-go-service/internal/ratelimit"
	"net/http"
)

func RateLimitApiHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK: запрос прошёл")
}

func RateLimitHtmlHandler(w http.ResponseWriter, r *http.Request) {
	html := `
<html>
<body>
  <h1>Тест Token Bucket</h1>
  <p><a href="/">index</a> 
  <p>Нажми кнопку 10+ раз подряд — увидишь 429.</p>
  <button id="btn">Нажать 10 раз быстро</button>

  <p>
Token Bucket («ведро токенов») — это алгоритм ограничения частоты запросов (rate limiting), который позволяет пропускать всплески нагрузки, но в среднем держит трафик в заданных рамках.
</p>
  <pre id="log"></pre>
  <script>
    const log = document.getElementById('log');
    function logLine(txt) { log.textContent += txt + '\\n'; }
    document.getElementById('btn').addEventListener('click', async () => {
      for (let i = 0; i < 30; i++) {
        const t0 = Date.now();
        try {
          const res = await fetch('/health');
          const text = await res.text();
          logLine(i +": " + res.status +" - " + text);
        } catch (e) {
          logLine(i +": error " +  e.message);
        }
        // Небольшая задержка, чтобы не упереться в лимиты браузера
        await new Promise(r => setTimeout(r, 50));
      }
    });
  </script>
</body>
</html>
`
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(html))
}

// func main() {
// 	// 5 запросов в секунду, всплеск до 10
// 	tb := ratelimit.NewTokenBucket(5, 10)

// 	mux := http.NewServeMux()
// 	mux.HandleFunc("/hello", helloHandler)
// 	mux.HandleFunc("/", testPageHandler)

// 	handler := ratelimit.RateLimitMiddleware(tb)(mux)

// 	log.Println("Server started on :8080")
// 	log.Fatal(http.ListenAndServe(":8080", handler))
// }
