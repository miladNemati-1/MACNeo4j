//import from csvs steps 






//1
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (park:Park), (animals:Wildlife) WHERE park.parkname = animals.parkname MERGE (park) -[:has]-> (animals)
WITH animals,park,wildlife,parks 

//2
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (animals) WHERE wildlife.Category  <> "NA"
MERGE (category: Category {category: wildlife.Category})
MERGE (animals) -[:category_is]-> (category)
WITH animals,park,wildlife,parks    

//3
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (animals) WHERE wildlife.Order  <> "NA"
MERGE (order: Order {ordername: wildlife.Order})
MERGE (animals) -[:order_is]-> (order)
WITH  animals,park,wildlife,parks


//4
MATCH (animals) WHERE wildlife.Family <> "NA"
MERGE (family: Family {familyname: wildlife.Family})
MERGE (animals) -[:family_is]-> (family)
WITH  animals,park,wildlife,parks


MATCH (animals) WHERE wildlife.ConservationStatus <> "NA"
MERGE (conservationstatus: ConservationStatus {conservationstatusname: wildlife.ConservationStatus})
MERGE (animals) -[:conservation_status_is]-> (conservationstatus)
WITH  animals,park,wildlife,parks
//5

LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife

MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (animals) WHERE wildlife.Seasonality <> "NA"
MERGE (seasonality: Seasonality {seasonalityname: wildlife.Seasonality})
MERGE (animals) -[:seasonality_is]-> (seasonality)
//6
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (animals) WHERE wildlife.Nativeness <> "NA"
MERGE (nativeness: Nativeness {nativenessname: wildlife.Nativeness})
MERGE (animals) -[:nativeness_is]-> (nativeness)
WITH  animals,park,wildlife,parks
//7

LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (animals) WHERE wildlife.Occurrence <> "NA"
MERGE (occurrence: Occurrence {occurencename: wildlife.Occurrence})
MERGE (animals) -[:occurrence_is]-> (occurrence)
WITH animals,park,wildlife,parks

//8
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife

MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 
MATCH (animals) WHERE wildlife.Seasonality <> "NA"
MERGE (seasonality: Seasonality {seasonalityname: wildlife.Seasonality})
MERGE (animals) -[:seasonality_is]-> (seasonality)
WITH  animals,park,wildlife,parks


///9
LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife

MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 
MATCH (animals) WHERE wildlife.Abundance <> "NA"
MERGE (abundance: Abundance {abundancename: wildlife.Abundance})
MERGE (animals) -[:abundance_is]-> (abundance)













//////ANd then 

MATCH (p:Park), (w:Wildlife) WHERE p.parkname = w.wildlife MERGE (p) -[:has]-> (w)

//////ANd then 





///////Count the number of species in each family. Display the counting result in descending order and limit the top 5 in the output


MATCH (animal:Wildlife)-[:family_is]->(family:Family) WITH family, count(animal) as NoSpecies RETURN family, NoSpecies ORDER BY NoSpecies DESC LIMIT 5

/////// Show all unique ScientificNames that with conditions of ‘BIRD’ category, ‘PASSERIFORMES’ order and ‘PRESENT’ occurrence in ascending order and lower case

MATCH (animal:Wildlife)-[a:category_is]->(Category: Category ) WHERE Category.category = "BIRD"
MATCH (animal)-[o:order_is]->(order: Order ) WHERE order.ordername = "PASSERIFORMES" 
MATCH (animal) WHERE animal.occurrence = "PRESENT" RETURN DISTINCT toLower(animal.scientificName) ORDER BY toLower(animal.scientificName)

//////Count the number of species of each Park with a range of 90,000 - 210,000 (both inclusive) AreaAcres


MATCH (park:Park)-[h:has]->(a: Wildlife )  WHERE park.area <= 210000 AND park.area >= 90000 WITH park, count(a) as noSpecies  RETURN park, noSpecies

///List all unique categories that contain ‘YELLOW’ as part of the content in CommonNames.

MATCH (animal:Wildlife)-[ci:category_is]->(c: Category ) WHERE ANY (name IN animal.commonames WHERE name=~ '(?i).*YELLOW.*') RETURN DISTINCT c;

///////







