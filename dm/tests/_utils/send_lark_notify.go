package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	url := os.Getenv("FEISHU_BOT_WEBHOOK_URL")
	repo := os.Getenv("GITHUB_REPOSITORY")
	committer := os.Getenv("GIT_COMMITTER_NAME")
	jobID := os.Getenv("GITHUB_RUN_ID")
	jobURL := fmt.Sprintf("https://github.com/%s/actions/runs/%s", repo, jobID)
	msgTMPL := `
	{
		"msg_type": "post",
		"content": {
		  "post": {
			"zh_cn": {
			  "title": "FIX ME",
			  "content": [
				[
				  {
					"tag": "text",
					"text": "repo :"
				  },
				  {
					"tag": "text",
					"text": "%s"
				  }
				],
				[
				  {
					"tag": "text",
					"text": "committer :"
				  },
				  {
					"tag": "text",
					"text": "%s"
				  }
				],
				[
				  {
					"tag": "text",
					"text": "url :"
				  },
				  {
					"tag": "a",
					"text": "link",
					"href": "%s"
				  }
				]
			  ]
			}
		  }
		}
	  }`
	jsonStr := fmt.Sprintf(msgTMPL, repo, committer, jobURL)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(jsonStr)))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("response Body:", string(body))
}
