# HARVEST-INPI

[![Docker Image CI](https://github.com/dataesr/harvest-inpi/actions/workflows/docker-image.yml/badge.svg)](https://github.com/dataesr/harvest-inpi/actions/workflows/docker-image.yml)

## Release

It uses [semver](https://semver.org/).

To create a new release:
```shell
make release VERSION=x.x.x
```

### Endpoints


```/harvest_compute_split```  
Harvest the INPI database with three jobs 
* Download and unzip new INPI files
* Load the mongo db with new entries without history
* Load the mongo db with new entries with history

```/mongo_reload_force```  
Reset the mongo db and force the load of specifed years with two jobs  
args={"force_years": [list_of_years_as_string] (empty = all years)
* Reload the mongo db with entries without history
* Reload the mongo db with entries with history

```/mongo_reload_force_no_history```  
Reset the mongo db and force the load of specifed years  
args={"force_years": [list_of_years_as_string] (empty = all years)
* Reload the mongo db with entries without history

```/mongo_reload_force_with_history```  
Reset the mongo db and force the load of specifed years  
args={"force_years": [list_of_years_as_string] (empty = all years)
* Reload the mongo db with entries with history
