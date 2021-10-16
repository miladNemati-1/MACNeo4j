LOAD CSV WITH HEADERS FROM "file:///wildlife_a2.csv" as wildlife
LOAD CSV WITH HEADERS FROM "file:///parks_a2.csv" as parks 
WITH parks, wildlife
MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

MATCH (park:Park), (animals:Wildlife) WHERE park.parkname = animals.parkname MERGE (park) -[:has]-> (animals)
WITH animals,park,wildlife,parks 


MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

WHERE wildlife.Category  <> "NA"
MERGE (category: Category {category: wildlife.Category})
MERGE (animals) -[:category_is]-> (category)
WITH animals,park,wildlife,parks  


MERGE (park: Park {parkid: parks.ParkID, area: toInteger(parks.AreaInAcres), latitude: toInteger(parks.Latitude), longitude: toInteger(parks.Longitude), statecode: parks.StateCode, parkestday: toInteger(parks.ParkEstDay), parkestmonth: toInteger(parks.ParkEstMonth), parkestyear: toInteger(parks.ParkEstYear), parkname: parks.ParkName})
MERGE (animals: Wildlife {speciesid: wildlife.SpeciesID, parkname: wildlife.ParkName, scientificName: wildlife.ScientificName, commonames: Split(wildlife.CommonNames, ","), recordstatus: wildlife.RecordStatus})
WITH animals,park,wildlife,parks 

WHERE wildlife.Order  <> "NA"
MERGE (order: Order {ordername: wildlife.Order})
MERGE (animals) -[:order_is]-> (order)
WITH  animals,park,wildlife,parks

WHERE wildlife.Family <> "NA"
MERGE (family: Family {familyname: wildlife.Family})
MERGE (animals) -[:family_is]-> (family)
WITH  animals,park,wildlife,parks


WHERE wildlife.ConservationStatus <> "NA"
MERGE (conservationstatus: ConservationStatus {conservationstatusname: wildlife.ConservationStatus})
MERGE (animals) -[:conservation_status_is]-> (conservationstatus)
WITH  animals,park,wildlife,parks

WHERE wildlife.Seasonality <> "NA"
MERGE (seasonality: Seasonality {seasonalityname: wildlife.Seasonality})
MERGE (animals) -[:seasonality_is]-> (seasonality)
WITH  animals,park,wildlife,parks

WHERE wildlife.Occurrence <> "NA"
MERGE (occurrence: Occurrence {occurencename: wildlife.Occurrence})
MERGE (animals) -[:occurrence_is]-> (occurrence)
WITH animals,park,wildlife,parks

WHERE wildlife.Seasonality <> "NA"
MERGE (seasonality: Seasonality {seasonalityname: wildlife.Seasonality})
MERGE (animals) -[:seasonality_is]-> (seasonality)
WITH  animals,park,wildlife,parks

WHERE wildlife.Abundance <> "NA"
MERGE (abundance: Abundance {abundancename: wildlife.Abundance})
MERGE (animals) -[:abundance_is]-> (abundance)
