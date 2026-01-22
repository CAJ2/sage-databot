## OpenStreetMap

OSM data is used to extract any useful mapping data related to the circular economy.

https://taginfo.openstreetmap.org/tags

### Waste/Recycling Disposal

Places where users can bring their waste are important information for the places table.

Useful links:
https://wiki.openstreetmap.org/wiki/Tag%3Aamenity%3Drecycling
https://wiki.openstreetmap.org/wiki/Waste_Processing
https://wiki.openstreetmap.org/wiki/Household_waste_in_the_United_Kingdom
https://wiki.openstreetmap.org/wiki/WikiProject_CircularEconomy

The data is extracted by matching a big set of tag filters:

- amenity=waste_basket
- amenity=waste_disposal
- amenity=waste_transfer_station
- amenity=recycling
- waste=trash
- waste=mixed
- recycling_type=container
- recycling_type=centre
- recycling:glass_bottles=yes/no
- recycling:paper=yes/no
- recycling:plastic=yes/no
- recycling:clothes=yes/no
- recycling:glass=yes/no
- recycling:cans=yes/no
- recycling:plastic_bottles=yes/no
- recycling:plastic_packaging=yes/no
- recycling:cardboard=yes/no
- recycling:waste=yes
- recycling:shoes=yes
- recycling:green_waste=yes/no
- recycling:paper_packaging=yes/no
- recycling:beverage_cartons=yes/no
- recycling:scrap_metal=yes/no
- recycling:newspaper=yes/no
- recycling:magazines=yes/no
- recycling:batteries=yes/no
- recycling:organic=yes/no
- recycling:garden_waste=yes/no
- recycling:food_waste=yes/no

Currently we store all the information for all objects matching one of these tags in the osm field as a small PBF.
