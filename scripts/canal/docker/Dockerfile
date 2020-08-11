FROM canal/osbase:v2

WORKDIR /
RUN yum -y install mysql
RUN wget https://github.com/alibaba/canal/releases/download/canal-1.1.5-alpha-1/canal.adapter-1.1.5-SNAPSHOT.tar.gz
RUN tar xf canal.adapter-1.1.5-SNAPSHOT.tar.gz
COPY . .
RUN ls ./

CMD ["./start.sh"]