import ee


class RasterDataset:

    def __init__(self, snippet, band, name=None, categorical=False, map_params={}):
        """
        Represent a Google Earth Engine raster dataset and the information we need to use it.

        snippet: The ee.Image or ee.ImageCollection object representing the desired dataset.
            This is the snippet of code provided on the dataset's page in the Earth Engine catalog.
            e.g.: ee.Image('path') or ee.ImageCollection('path')
        band: The identifier of the desired image band e.g. 'bio01'
        name: The column name to display for the data gathered from this dataset
        categorical: Set to True if the data is not continuous. If True, takes the mode of pixels
            in the sample area rather than the median.
        map_params: Settings for how to display the data e.g. palette, min, max.
            See param options: https://developers.google.com/earth-engine/api_docs#ee.data.getmapid
        """

        # self.name exists to be a common property between RasterDatasets and VectorDatasets
        self.name = name or band  # If a different name is not set, use the band name
        self.categorical = categorical
        self.map_params = map_params

        # Select just the one band of interest from the image and rename it to self.name
        if isinstance(snippet, ee.ImageCollection):
            # If it's an image collection, mosaic it into one image
            self.data = snippet.select([band], [self.name]).mosaic()
        else:
            self.data = snippet.select([band], [self.name])

        self.band = self.name

    def get_sample_area_data(self, sample_area: ee.Feature) -> ee.Feature:
        """
        Add a new property to the input feature storing the value of self.data in the feature area

        param sample_area: ee.Feature with the geometry to sample over
        returns: input feature with a new property in the form
            {self.band: mode (categorical) or median (continuous) of self.data
                        over the feature geometry}
        """

        # Get the average distance from each point in the sample area to the data image
        distance = self.distance(sample_area.geometry())

        geometry = ee.Algorithms.If(
            distance,  # If the distance is not None,
            ee.Algorithms.If(
                distance.eq(0),  # If the distance is 0, the data covers the sample area
                sample_area.geometry(),
                sample_area.geometry().buffer(distance), # Otherwise, we need to buffer it
            ),
            # If the distance is None, the sample area is farther away from the data than the
            # range being searched by the distance kernel.
            # Continue with the sample area geometry, the reducer will return None.
            sample_area.geometry()
        )
        result = ee.Algorithms.If(
            self.categorical,  # If the data type is categorical,
            self.mode(geometry),  # Get the most common category in the sample area
            self.median(geometry)  # Otherwise it's continuous, so get the median
        )

        return sample_area.set(self.band, result)

    def distance(self, geometry: ee.Geometry) -> ee.Number:
        """
        Return the average distance from each point in the input geometry to the self.data image

        - This should be 0 in most cases, meaning the data covers the sample area.
        - If the sample area is within the search radius from the self.data image
            (here set to 1000m), the distance will be returned.
        - If the sample area is outside the search radius, None is returned.

        This is intended to account for edges of datasets not lining up exactly with the
        coastline, leaving some coastal sample sites just outside the self.data image.

        param geometry: ee.Geometry of the sample area of interest
        returns: ee.Number (0 or distance) or None
        """
        # Make an image representing the distance from any given point to the self.data image
        # This will be 0 everywhere that the self.data image covers
        distance_image = self.data.distance(
            ee.Kernel.euclidean(1000, 'meters'),
            False  # Do not exclude masked pixels
        ).select([self.band], ['distance'])  # Rename the band to 'distance'

        # Get the average distance from each point in the sample area to the nearest pixel of data
        distance = ee.Number(distance_image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geometry,
            scale=100,
            maxPixels=1e9
        ).get('distance'))

        return distance

    def mode(self, geometry: ee.Geometry) -> ee.Number:
        """
        Reduce the dataset over the sample area to get the most common value in that area.

        param geometry: ee.Geometry of the sample area of interest
        returns: mode of pixels within the geometry as an ee.Number
        """
        mode = self.data.reduceRegion(
            reducer=ee.Reducer.mode(),
            geometry=geometry,
            scale=100,
            maxPixels=1e9
        ).get(self.band)
        return mode

    def median(self, geometry: ee.Geometry) -> ee.Number:
        """
        Reduce the dataset over the sample area to get the median value in that area

        param geometry: ee.Geometry of the sample area of interest
        returns: median of pixels within the geometry as an ee.Number
        """
        median = self.data.reduceRegion(
            reducer=ee.Reducer.median(),
            geometry=geometry,
            scale=100,
            maxPixels=1e9
        ).get(self.band)
        return median


class VectorDataset:
    def __init__(self, snippet, property: str, name=None, map_params={}):
        """
        Represent a Google Earth Engine vector dataset and the information we need to use it.

        snippet: The ee.FeatureCollection object representing the dataset.
            This is the 'Earth Engine snippet' displayed on the dataset's page in the Earth
            Engine catalog. e.g.: ee.FeatureCollection('EPA/Ecoregions/2013/L3')
        property: The key of the desired feature property
        name: The column name to display for the data gathered from this dataset.
        map_params: Settings for how to display the data e.g. palette, min, max
        """
        self.data = snippet
        self.property = property
        self.name = name or property
        self.map_params = map_params

    # Note: This function is an argument to map(). Arguments to map() cannot print anything
    # or call getInfo(). Doing so results in an EEException: ValueNode empty
    # source: https://gis.stackexchange.com/questions/345598/mapping-simp>le-function-to-print-date-and-time-stamp-on-google-earth-engine-pyth
    def get_sample_area_data(self, sample_area: ee.Feature) -> str:
        """
        Return the value from the dataset to assign to the sample area.
        """
        # Get a FeatureCollection storing the overlaps between the sample area and the dataset
        overlaps = self.data.filterBounds(sample_area.geometry()).map(
            lambda feature: feature.intersection(sample_area.geometry())
        )

        result = ee.Algorithms.If(
            # If there is exactly 1 overlapping dataset feature, return its value
            overlaps.size().eq(1),
            sample_area.set(self.name, overlaps.first().get(self.property)),

            ee.Algorithms.If(
                # If there are 0 overlapping dataset features, return the value of the closest one
                overlaps.size().eq(0),
                sample_area.set(self.name,
                                self.get_nearest_feature(sample_area).get(self.property)),

                # Otherwise, there must be >1 features overlapping the sample area
                # Return the value of the one with the largest overlap
                sample_area.set(self.name,
                                self.get_predominant_feature(overlaps).get(self.property))
            )
        )

        return result

    def get_nearest_feature(self, sample_area: ee.Feature) -> str:
        """
        To be used when the sample area doesn't overlap the dataset at all.

        Get the dataset feature that is nearest to the sample area, and
        return the value of its dataset.property.

        param sample_area: ee.Feature representing the sample area of interest
        """

        # Define a filter to get all dataset features within 10000 meters of the sample area
        spatialFilter = ee.Filter.withinDistance(
            distance=10000,
            leftField='.geo',
            rightField='.geo',
            maxError=10
        )
        # Define a join that will return only the 'best' (nearest) match
        saveBestJoin = ee.Join.saveBest(
          matchKey='closestFeature',
          measureKey='distance'
        )
        # Apply the join, using the distance filter to define match quality
        # Get the only feature in the resulting FeatureCollection
        result = ee.Feature(saveBestJoin.apply(
            ee.FeatureCollection(sample_area),
            self.data,
            spatialFilter
        ).first())

        # Return the closest dataset feature
        return ee.Feature(result.get('closestFeature'))

    def get_predominant_feature(self, overlaps: ee.FeatureCollection) -> str:
        """
        To be used when the sample area overlaps more than one dataset feature.

        Return the value of 'property' for the largest overlap.
        """
        # Add 'area' as a property to each feature. This is the area in square meters
        # of the intersection of the ecoregion feature and the sample area.
        overlaps = overlaps.map(
            lambda feature: feature.set({'area': feature.geometry().area()}))

        # Find the maximum area among all the overlaps
        max_area = overlaps.aggregate_max('area')

        # Return the overlap with the largest area
        return ee.Feature(overlaps.filter(ee.Filter.gte('area', max_area)).first())
