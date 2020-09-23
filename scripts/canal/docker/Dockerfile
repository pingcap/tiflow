FROM canal/osbase:v2

WORKDIR /root
RUN wget https://github.com/alibaba/canal/releases/download/canal-1.1.5-alpha-2/canal.adapter-1.1.5-SNAPSHOT.tar.gz
RUN tar xf canal.adapter-1.1.5-SNAPSHOT.tar.gz
COPY . .
RUN chmod 777 start.sh

CMD ["/bin/sh", "./start.sh"]
