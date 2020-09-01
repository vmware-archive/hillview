This directory contains metadata that ties columns in
the browsed datasets to geographic information descriptions.
Each directory corresponds to a dataset, and it contains a
geometa.json file that has an array of json objects with
a schema corresponding to the following TypeScript interface:

export interface ColumnGeoRepresentation {
    columnName: string; // e.g., OriginState
    geoFile: string; // relative to the geo directory; e.g., geo/us_states/cb_2019_us_state_20m.shp
    property: string; // which property in the dataset is indexed by values in the column. e.g., STUSPS
    projection: string; // one of the supported data projections
    // Legal projection names are:
    // geoAzimuthalEqualArea
    // geoAzimuthalEquidistant
    // geoGnomonic
    // geoOrthographic
    // geoStereographic
    // geoEqualEarth
    // geoAlbersUsa
    // geoConicEqualArea
    // geoConicEquidistant
    // geoEquirectangular
    // geoMercator
    // geoTransverseMercator
    // geoNaturalEarth1
}