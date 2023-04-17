import functools
import os
from datetime import datetime
from multiprocessing import Pool

import ee
import folium
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from datasets import RasterDataset, VectorDataset


def initialize_ee(service_account: str, auth=False):
    """ Intialize Earth Engine """
     # TODO this needs to need to run outside of google drive/colab context
    credentials = ee.ServiceAccountCredentials(
        service_account,
        './drive/MyDrive/Colab Notebooks/edna-explorer-2e3e8d7c527a.json'
    )

    if auth:
        ee.Authenticate()
    ee.Initialize(credentials)


def get_kroeker_md_input_subset():
    """"""
    # Get current working directory on your Google Drive.
    CurrentDirectory = os.getcwd()

    # Specify the directory on Google Drive that you want to use for
    # accessing your metadata.
    ProjectDirectory = "/drive/MyDrive/Colab Notebooks/"

    # Specify metadata filename.
    MetadataInput = "KroekerMetadataInputSubset.csv"

    #Specify full metadata file input path
    csv_path = CurrentDirectory+ProjectDirectory+MetadataInput

    # Read the relevant columns from the CSV into a dataframe
    samples = pd.read_csv(csv_path, usecols=['sample_id', 'longitude', 'latitude', 'sample_date', 'spatial_uncertainty'])
    samples = samples.astype({'sample_id': 'object', 'longitude': 'float64', 'latitude': 'float64', 'sample_date': 'object', 'spatial_uncertainty': 'float64'})

    # Remove entries with missing data
    samples = samples.dropna()
    return samples


def define_feature_for_sample_areas(samples, br):
    """Define a Feature for each sample area within the spatial uncertainty of each sample location.

    :param samples:
    :param br: Buffer radius
    """
    sample_areas = []
    for sample in samples.itertuples():
        # Store the important data as properties of the feature
        sample_areas.append(
            ee.Feature(ee.Geometry.Point(sample.longitude, sample.latitude).buffer(br))
            .set('name', sample.sample_id)
            .set('sample_date', sample.sample_date)
            .set('longitude', sample.longitude)
            .set('latitude', sample.latitude)
            .set('spatial_uncertainty', sample.spatial_uncertainty))

    # Define a FeatureCollection containing all the sample areas
    return ee.FeatureCollection(sample_areas)


def instantiate_datasets():
    """"""
    bioclim = [
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO').divide(ee.Image(10)),
            band='bio01'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO').divide(ee.Image(10)),
            band='bio02'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO'),
            band='bio03'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO').divide(ee.Image(100)),
            band='bio04'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO').divide(ee.Image(10)),
            band='bio05'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO').divide(ee.Image(10)),
            band='bio06'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO').divide(ee.Image(10)),
            band='bio07'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO').divide(ee.Image(10)),
            band='bio08'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO').divide(ee.Image(10)),
            band='bio09'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO').divide(ee.Image(10)),
            band='bio10'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO').divide(ee.Image(10)),
            band='bio11'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO'),
            band='bio12'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO'),
            band='bio13'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO'),
            band='bio14'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO'),
            band='bio15'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO'),
            band='bio16'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO'),
            band='bio17'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO'),
            band='bio18'
        ),
        RasterDataset(
            snippet=ee.Image('WORLDCLIM/V1/BIO'),
            band='bio19'
        )
    ]

    soil = [
        RasterDataset(  # Soil taxonomy great groups, a classification number from 0 to 433
            snippet=ee.Image('OpenLandMap/SOL/SOL_GRTGROUP_USDA-SOILTAX_C/v01'),
            band='grtgroup',
            categorical=True
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02').divide(ee.Image(10)),
            band='b0',
            name='soil pH in H20 at 0 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02').divide(ee.Image(10)),
            band='b10',
            name='soil pH in H20 at 10 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02').divide(ee.Image(10)),
            band='b30',
            name='soil pH in H20 at 30 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02').divide(ee.Image(10)),
            band='b60',
            name='soil pH in H20 at 60 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02').divide(ee.Image(10)),
            band='b100',
            name='soil pH in H20 at 100 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02').divide(ee.Image(10)),
            band='b200',
            name='soil pH in H20 at 200 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02').divide(ee.Image(5)),
            band='b0',
            name='soil organic carbon at 0 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02').divide(ee.Image(5)),
            band='b10',
            name='soil organic carbon at 10 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02').divide(ee.Image(5)),
            band='b30',
            name='soil organic carbon at 30 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02').divide(ee.Image(5)),
            band='b60',
            name='soil organic carbon at 60 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02').divide(ee.Image(5)),
            band='b100',
            name='soil organic carbon at 100 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02').divide(ee.Image(5)),
            band='b200',
            name='soil organic carbon at 200 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_SAND-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b0',
            name='soil sand at 0 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_SAND-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b10',
            name='soil sand at 10 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_SAND-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b30',
            name='soil sand at 30 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_SAND-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b60',
            name='soil sand at 60 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_SAND-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b100',
            name='soil sand at 100 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_SAND-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b200',
            name='soil sand at 200 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b0',
            name='soil clay at 0 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b10',
            name='soil clay at 10 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b30',
            name='soil clay at 30 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b60',
            name='soil clay at 60 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b100',
            name='soil clay at 100 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02'),
            band='b200',
            name='soil clay at 200 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_BULKDENS-FINEEARTH_USDA-4A1H_M/v02').divide(ee.Image(10)),
            band='b0',
            name='soil bulk density at 0 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_BULKDENS-FINEEARTH_USDA-4A1H_M/v02').divide(ee.Image(10)),
            band='b10',
            name='soil bulk density at 10 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_BULKDENS-FINEEARTH_USDA-4A1H_M/v02').divide(ee.Image(10)),
            band='b30',
            name='soil bulk density at 30 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_BULKDENS-FINEEARTH_USDA-4A1H_M/v02').divide(ee.Image(10)),
            band='b60',
            name='soil bulk density at 60 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_BULKDENS-FINEEARTH_USDA-4A1H_M/v02').divide(ee.Image(10)),
            band='b100',
            name='soil bulk density at 100 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_BULKDENS-FINEEARTH_USDA-4A1H_M/v02').divide(ee.Image(10)),
            band='b200',
            name='soil bulk density at 200 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_TEXTURE-CLASS_USDA-TT_M/v02'),
            band='b0',
            name='soil texture class at 0 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_TEXTURE-CLASS_USDA-TT_M/v02'),
            band='b10',
            name='soil texture class at 10 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_TEXTURE-CLASS_USDA-TT_M/v02'),
            band='b30',
            name='soil texture class at 30 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_TEXTURE-CLASS_USDA-TT_M/v02'),
            band='b60',
            name='soil texture class at 60 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_TEXTURE-CLASS_USDA-TT_M/v02'),
            band='b100',
            name='soil texture class at 100 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_TEXTURE-CLASS_USDA-TT_M/v02'),
            band='b200',
            name='soil texture class at 200 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_WATERCONTENT-33KPA_USDA-4B1C_M/v01'),
            band='b0',
            name='Percent volume soil water content at 0 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_WATERCONTENT-33KPA_USDA-4B1C_M/v01'),
            band='b10',
            name='Percent volume soil water content at 10 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_WATERCONTENT-33KPA_USDA-4B1C_M/v01'),
            band='b30',
            name='Percent volume soil water content at 30 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_WATERCONTENT-33KPA_USDA-4B1C_M/v01'),
            band='b60',
            name='Percent volume soil water content at 60 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_WATERCONTENT-33KPA_USDA-4B1C_M/v01'),
            band='b100',
            name='Percent volume soil water content at 100 cm'
        ),
        RasterDataset(
            snippet=ee.Image('OpenLandMap/SOL/SOL_WATERCONTENT-33KPA_USDA-4B1C_M/v01'),
            band='b200',
            name='Percent volume soil water content at 200 cm'
        )
    ]

    terrain = [
        RasterDataset(
            snippet=ee.Image('CGIAR/SRTM90_V4'),
            band='elevation'
        ),
        RasterDataset(
            snippet=ee.Terrain.slope(ee.Image('CGIAR/SRTM90_V4')),
            band='slope'
        ),
        RasterDataset(
            snippet=ee.Terrain.aspect(ee.Image('CGIAR/SRTM90_V4')),
            band='aspect'
        ),
    ]

    hii = [
        RasterDataset(
            snippet=ee.ImageCollection('CSP/HM/GlobalHumanModification'),
            band='gHM'
        ),
    ]

    landsat = [
        RasterDataset(
            snippet=ee.ImageCollection('LANDSAT/LC08/C01/T1_32DAY_NDVI').filterDate(
                EarliestTime, LatestTime
            ),
            band='NDVI',
            map_params={'min': 0, 'max': 1, 'palette': [
        'FFFFFF', 'CE7E45', 'DF923D', 'F1B555', 'FCD163', '99B718', '74A901',
        '66A000', '529400', '3E8601', '207401', '056201', '004C00', '023B01',
        '012E01', '011D01', '011301'
    ]}
        ),
        RasterDataset(
            snippet=ee.ImageCollection('LANDSAT/LC08/C01/T1_8DAY_EVI').filterDate(
                EarliestTime, LatestTime
            ),
            band='EVI'
        ),
        RasterDataset(
            snippet=ee.ImageCollection('LANDSAT/LC08/C01/T1_8DAY_NBRT').filterDate(
                EarliestTime, LatestTime
            ),
            band='NBRT'
        ),
        RasterDataset(
            snippet=ee.ImageCollection('LANDSAT/LC08/C01/T1_ANNUAL_GREENEST_TOA').filterDate(
                EarliestTime, LatestTime
            ),
            band='greenness'
        )
    ]

    return bioclim, soil, terrain, hii, landsat


# Here is an extra function for reprojecting an Image to a different scale.
# Might be useful for something, not sure what effect this has exactly

def reproject(image: ee.Image, scale: int) -> ee.Image:
    '''
    Resample and reproject an image to a different resolution.

    param image: Image to reproject
    param scale: Desired pixel width in meters
    '''
    return image.resample('bilinear').reproject(  # Use bilinear interpolation method
        crs=image.projection().crs(),  # Keep the same map projection
        scale=scale  # Change the scale
    )


def main():
    initialize_ee()
    samples = get_kroeker_md_input_subset()

    # Calculate a buffer from the spatial uncertainty in meters.
    buffer_radius = samples['spatial_uncertainty'].median()

    #Read in sample date as a date.
    samples['sample_date']= pd.to_datetime(samples['sample_date'])

    #Define earliest and latest times using sample dates with a six +/- month buffer.
    EarliestTime = min(samples['sample_date']) - relativedelta(months=6)
    LatestTime = max(samples['sample_date']) + relativedelta(months=6)

    #Define geographic bounds using sample coordinates and a +/- 0.5 degree buffer.
    EastBound = max(samples['longitude']) + 0.5
    WestBound = min(samples['longitude']) - 0.5
    SouthBound = min(samples['latitude']) - 0.5
    NorthBound = max(samples['latitude']) + 0.5

    # Display the shape of the data we read in and its first few rows
    print("Data shape:", samples.shape)
    print(samples.head())

    sample_areas = define_feature_for_sample_areas(samples, buffer_radius)
