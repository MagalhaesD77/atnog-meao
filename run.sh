helm -n meao uninstall osm-mec
docker build -t localhost:5000/cfs-portal cfs-portal
docker push localhost:5000/cfs-portal
docker build -t localhost:5000/meao meao
docker push localhost:5000/meao
docker build -t localhost:5000/oss oss
docker push localhost:5000/oss
helm -n meao upgrade --install osm-mec osm-mec-helm-chart
kubectl get pods -A