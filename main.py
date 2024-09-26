import http.server
import requests
import yaml

CONFIG_FILE = "config.yaml"

def load_config():
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

class KeywordFilterHandler(http.server.BaseHTTPRequestHandler):
    config = load_config()
    previous_keyword = set(config['keyword']) 

    def load_config(self):
        config = load_config()
        self.keyword = set(config['keyword'])
        self.forward_url = config.get('forward_url', '')
        self.receive_url = config.get('receive_url', '')

        added_keyword = set(self.keyword) - set(self.previous_keyword)
        removed_keyword = set(self.previous_keyword) - set(self.keyword)

        if added_keyword or removed_keyword:
            if added_keyword:
                print(f"已新增关键词：[{', '.join(added_keyword)}]")
            if removed_keyword:
                print(f"已删去关键词：[{', '.join(removed_keyword)}]")
        
        self.previous_keyword = self.keyword

    def do_POST(self):
        self.load_config()
        
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        matched_keyword = [keyword for keyword in self.keyword if keyword in post_data]

        if matched_keyword:
            print(f"匹配到关键词：[{', '.join(matched_keyword)}]，转发至：[{self.forward_url}]")
            response = requests.post(self.forward_url, data=post_data.encode('utf-8'), headers={'Content-Type': 'application/json; charset=utf-8'})
            self.send_response(response.status_code)
            self.end_headers()
            self.wfile.write(response.content)
        else:
            print(f"未匹配到关键词，忽略！")

def run():
    config = load_config()
    port = int(config['receive_url'].split(':')[-1])
    server_address = ('', port)
    print(f"已配置关键词：{config['keyword']}\n已配置监听地址：{config['receive_url']}\n已配置转发地址：{config['forward_url']}")
    httpd = http.server.HTTPServer(server_address, KeywordFilterHandler)
    print(f'开始监听端口：{port}\nKira~☆')
    httpd.serve_forever()

if __name__ == "__main__":
    run()
