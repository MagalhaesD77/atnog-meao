helm -n meao uninstall ue-lat-sim
docker build -t localhost:5000/ue-lat-sim .
docker push localhost:5000/ue-lat-sim
helm -n meao upgrade --install ue-lat-sim ue-lat-sim-helm-chart
kubectl get pods -A