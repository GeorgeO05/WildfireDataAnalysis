mvn clean package
spark-submit --class edu.ucr.cs.cs167.Project20_finalproject.BeastScala --master "local[*]" target/Project20_finalproject-1.0-SNAPSHOT.jar data-prep wildfiredb_1k.csv.bz2 wildfiredb1k_ZIP
spark-submit --class edu.ucr.cs.cs167.Project20_finalproject.BeastScala --master "local[*]" target/Project20_finalproject-1.0-SNAPSHOT.jar data-prep wildfiredb_10k.csv.bz2 wildfiredb10k_ZIP
spark-submit --class edu.ucr.cs.cs167.Project20_finalproject.BeastScala --master "local[*]" target/Project20_finalproject-1.0-SNAPSHOT.jar data-prep wildfiredb_100k.csv.bz2 wildfiredb100k_ZIP
spark-submit --class edu.ucr.cs.cs167.Project20_finalproject.BeastScala --master "local[*]" target/Project20_finalproject-1.0-SNAPSHOT.jar spatial-analysis wildfiredb10k_ZIP wildfireIntensityCounty 01/01/2016 12/31/2017
spark-submit --class edu.ucr.cs.cs167.Project20_finalproject.BeastScala --master "local[*]" target/Project20_finalproject-1.0-SNAPSHOT.jar temporal-analysis wildfiredb100k_ZIP wildfiresRiverside Riverside
