docker stop news-engine
docker rm news-engine
docker rmi news-engine
docker build -t news-engine .
docker run -itd  --name news-engine --network dockers_default news-engine
#docker network connect mongo news-engine
