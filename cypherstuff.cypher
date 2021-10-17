//import from csvs steps 






//1 ok
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: Split(parks.StateCode, ","), parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (park:Park), (animals:Wildlife) WHERE park.parkname = animals.parkname MERGE (park) -[:has]-> (animals)
 

//2 ok
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: Split(parks.StateCode, ","), parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (animals) WHERE wildlife.Category  <> "NA"
MERGE (category: Category {category: wildlife.Category})
MERGE (animals) -[:category_is]-> (category)


//3 ok 
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: Split(parks.StateCode, ","), parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (animals) WHERE wildlife.Order  <> "NA"
MERGE (order: Order {ordername: wildlife.Order})
MERGE (animals) -[:order_is]-> (order)



//4 ok 
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: Split(parks.StateCode, ","), parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH  animals,park,wildlife,parks

MATCH (animals) WHERE wildlife.Family <> "NA"
MERGE (family: Family {familyname: wildlife.Family})
MERGE (animals) -[:family_is]-> (family)

///5 ok 
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: Split(parks.StateCode, ","), parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH  animals,park,wildlife,parks

MATCH (animals) WHERE wildlife.ConservationStatus <> "NA"
MERGE (conservationstatus: ConservationStatus {conservationstatusname: wildlife.ConservationStatus})
MERGE (animals) -[:conservation_status_is]-> (conservationstatus)

//6 ok

LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife

MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: Split(parks.StateCode, ","), parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (animals) WHERE wildlife.Seasonality <> "NA"
MERGE (seasonality: Seasonality {seasonalityname: wildlife.Seasonality})
MERGE (animals) -[:seasonality_is]-> (seasonality)
//7 ok
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: Split(parks.StateCode, ","), parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (animals) WHERE wildlife.Nativeness <> "NA"
MERGE (nativeness: Nativeness {nativenessname: wildlife.Nativeness})
MERGE (animals) -[:nativeness_is]-> (nativeness)

//8 ok

LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: Split(parks.StateCode, ","), parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (animals) WHERE wildlife.Occurrence <> "NA"
MERGE (occurrence: Occurrence {occurencename: wildlife.Occurrence})
MERGE (animals) -[:occurrence_is]-> (occurrence)


//9XX
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife

MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: Split(parks.StateCode, ","), parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 
MATCH (animals) WHERE wildlife.Seasonality <> "NA"
MERGE (seasonality: Seasonality {seasonalityname: wildlife.Seasonality})
MERGE (animals) -[:seasonality_is]-> (seasonality)



///10
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife

MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: Split(parks.StateCode, ","), parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 
MATCH (animals) WHERE wildlife.Abundance <> "NA"
MERGE (abundance: Abundance {abundancename: wildlife.Abundance})
MERGE (animals) -[:abundance_is]-> (abundance)





///////1. Count the number of species in each family. Display the counting result in descending order and limit the top 5 in the output


MATCH (animal:Wildlife)-[:family_is]->(family:Family) WITH family, count(animal) as NoSpecies RETURN family, NoSpecies ORDER BY NoSpecies DESC LIMIT 5

///////2.  Show all unique ScientificNames that with conditions of ‘BIRD’ category, ‘PASSERIFORMES’ order and ‘PRESENT’ occurrence in ascending order and lower case

MATCH (animal:Wildlife)-[a:category_is]->(Category: Category ) WHERE Category.category = "BIRD"
MATCH (animal)-[o:order_is]->(order: Order ) WHERE order.ordername = "PASSERIFORMES" 
MATCH (animal)-[oc:occurrence_is]->(occurence: Occurrence ) WHERE occurence.occurencename = "PRESENT" RETURN DISTINCT toLower(animal.scientificName) ORDER BY toLower(animal.scientificName)

//////3. Count the number of species of each Park with a range of 90,000 - 210,000 (both inclusive) AreaAcres


MATCH (park:Park)-[h:has]->(a: Wildlife )  WHERE park.area <= 210000 AND park.area >= 90000 WITH park, count(a) as noSpecies  RETURN park, noSpecies

///4. List all unique categories that contain ‘YELLOW’ as part of the content in CommonNames.

MATCH (animal:Wildlife)-[ci:category_is]->(c: Category ) WHERE ANY (name IN animal.commonames WHERE name=~ '(?i).*YELLOW.*') RETURN DISTINCT c;

///////5. Mentuioned you thought about filtering to only compare to CA parks however some parks closest to PINN could be also in anohter state due to area 



MATCH (pinnPark:Park) WHERE pinnPark.parkid = "PINN" WITH pinnPark
MATCH (park:Park) WHERE park.parkid <> "PINN"
WITH point({longitude: pinnPark.longitude, latitude: pinnPark.latitude}) AS pointOne, point({longitude: park.longitude, latitude: park.latitude}) AS pointTwo, park, pinnPark
RETURN round(distance(pointOne, pointTwo)) AS distanceFromPinn, park, pinnPark  ORDER BY distanceFromPinn LIMIT 1

//// Average area in acres of each state


match  (park:Park)
unwind park.statecode as stateName
RETURN stateName  AS state, 
       round(AVG(toFloat(park.area)), 3) As avg




///top 3 states with most parks


match  (park:Park)
unwind park.statecode as stateName
RETURN stateName  AS state, 
       count(park.parkid) As noparks


//Index

CREATE INDEX category_index FOR (animal: Wildlife) ON (animal.category)

CREATE INDEX commonnames_index FOR (animal: Wildlife) ON (animal.commonnames)

CREATE INDEX order_index FOR (order: Order) ON (order.ordername)

CREATE INDEX area_index FOR (park: Park) ON (park.area)

CREATE INDEX statecode_index FOR (park: Park) ON (park.statecode)

CREATE INDEX parkid_index FOR (park: Park) ON (park.parkid)

////////////Insert data

///1

MERGE (p:Park{parkname: "Rainbow Wonderland"})
SET p.parkid = "RAWO"

MERGE (a:Wildlife{scientificName: "unicorn"})
SET a.speciesid= "00007"
SET a.commonames= ["unicorn"]


MERGE (f:Family{familyname: "unicorn family"})


MERGE (o:Order{ordername: "unicorn order"})


MERGE (c:Category{category: "mythical creature"})

MERGE (p)-[:has]->(a)
MERGE (a)-[:category_is]->(c)
MERGE (a)-[:order_is]->(o)
MERGE (a)-[:family_is]->(f)
RETURN a,p,c,o,f

//2 




MERGE (nia:Nativeness{nativenessname: "Near-Nativeness"}) 
WITH nia
MATCH (a: Wildlife) -[:category_is]->(c) WHERE c.category = "MAMMAL"
MATCH (a) -[ni:nativeness_is]->(n) WHERE n.nativenessname ="UNKNOWN"
DELETE ni
WITH a, c, n,nia
MERGE (a) -[:category_is]->(nia)


//3 


MATCH (p:Park)-[h:has]->(w:Wildlife) WHERE p.parkname = "MESA VERDE NATIONAL PARK" MATCH (w)-[ca:category_is]->(c:Category) 
MATCH (w)-[oi:occurrence_is]->(o:Occurrence) 
MATCH (w)-[ni:nativeness_is]->(n:Nativeness) 
MATCH (w)-[ordi:order_is]->(ord:Order) 
MATCH (w)-[fi:family_is]->(f:Family)
MATCH (w)-[ci:category_is]->(c:Category)  
MATCH (w)-[ai:abundance_is]->(a:Abundance)  
MATCH (w)-[si:seasonality_is]->(s:Seasonality)  
MATCH (w)-[csi:conservation_status_is]->(cs:ConservationStatus)  

DETACH DELETE p,w,c,o,n,ord,f,s,cs,a

