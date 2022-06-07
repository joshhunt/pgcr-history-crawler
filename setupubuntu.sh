cd ~
curl -sL https://deb.nodesource.com/setup_16.x -o /tmp/nodesource_setup.sh
bash /tmp/nodesource_setup.sh
apt install -y nodejs

npm install -g yarn
git clone https://github.com/joshhunt/pgcr-history-crawler.git
cd ~/pgcr-history-crawler
yarn



vim /etc/grafana-agent.yaml

systemctl start grafana-agent.service
systemctl status grafana-agent.service


vim /etc/netplan/50-cloud-init.yaml




vim .env

yarn run ts-node --esm ./main.ts





