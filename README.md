# ecco-api
API based on Quart Framework



```
PREFECT_API_URL=http://prefect-server:4200/api python3 admin.py deploy --deployment-name dhi_people_ecco --image-name dhi_people_ecco --image-tag v3 --dockerfile dhi_people_ecco.Dockerfile --src-base "dhi_people_ecco.src.main"
```


```
PREFECT_API_URL=http://prefect-server:4200/api python3 admin.py deploy --deployment-name hatfield_spectral_recovery --image-name hatfield_spectral_recovery --image-tag v1 --dockerfile hatfield_spectral_recovery.Dockerfile --src-base "hatfield.spectral-recovery.src.main"
```