helm -n meao uninstall thesis-project
docker build -t localhost:5000/meao-monitoring .
docker push localhost:5000/meao-monitoring
helm -n meao upgrade --install thesis-project Thesis-Project-helm-chart
kubectl get pods -A