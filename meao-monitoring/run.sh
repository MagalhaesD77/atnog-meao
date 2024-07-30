helm -n meao uninstall meao-monitoring
docker build -t localhost:5000/meao-monitoring .
docker push localhost:5000/meao-monitoring
helm -n meao upgrade --install meao-monitoring meao-monitoring-helm-chart
kubectl get pods -A