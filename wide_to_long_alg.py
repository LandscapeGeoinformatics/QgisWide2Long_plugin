# -*- coding: utf-8 -*-

"""
***************************************************************************
*                                                                         *
*   This program is free software; you can redistribute it and/or modify  *
*   it under the terms of the GNU General Public License as published by  *
*   the Free Software Foundation; either version 2 of the License, or     *
*   (at your option) any later version.                                   *
*                                                                         *
***************************************************************************
"""
from qgis.PyQt.QtCore import QCoreApplication
from qgis.PyQt.QtCore import QVariant

from qgis.core import (
            QgsProcessing,
            Qgis,
            QgsMessageLog,
            QgsProcessingException,
            QgsProcessingAlgorithm,
            QgsProcessingParameterNumber,
            QgsProcessingParameterBoolean,
            QgsProcessingParameterString,
            QgsProcessingParameterField,
            
            QgsField,
            QgsFields,
            QgsFeature,
            QgsFeatureSink,
            
            QgsProcessingParameterFeatureSource,
            QgsProcessingParameterFeatureSink)
            
from qgis import processing

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, MultiPolygon

class Wide2LongProcessingAlgorithm(QgsProcessingAlgorithm):
    """
    This is an Wide2Long algorithm that takes a vector layer and
    creates a new one.
    """

    # Constants used to refer to parameters and outputs. They will be
    # used when calling the algorithm from another algorithm, or when
    # calling from the QGIS console.

    INPUT = 'INPUT'
    
    OUTPUT = 'OUTPUT'
    
    # select those columns that should be copied (don't forget geometry)
    # base_columns_to_keep = ["SOVEREIGNT", "REGION_UN", "NAME_EN", "UN_code", "geometry" ]
    BASECOLSKEEP = 'BASECOLSKEEP'
    
    
    # a list, the columns that are currently in "wide" format, that should be transposed into row-wise/per feature long format
    WIDE2LONGCOLUMNS = 'WIDE2LONGCOLUMNS'
    
    # how to name the attribute that should represent the column title (has the value from the wide column name)
    # wide_to_long_orig_column_name_represent = "year"
    WIDE2LONGORIGCOLNAMEREPR = 'WIDE2LONGORIGCOLNAMEREPR'
    
    # how to name the attribute that gets the data value 
    # wide_to_long_transpose_data_column = "population"
    WIDE2LONGTRANSDATACOL = 'WIDE2LONGTRANSDATACOL'


    def tr(self, string):
        """
        Returns a translatable string with the self.tr() function.
        """
        return QCoreApplication.translate('Wide2Long', string)

    def createInstance(self):
        return Wide2LongProcessingAlgorithm()

    def name(self):
        """
        Returns the algorithm name, used for identifying the algorithm. This
        string should be fixed for the algorithm, and must not be localised.
        The name should be unique within each provider. Names should contain
        lowercase alphanumeric characters only and no spaces or other
        formatting characters.
        """
        return 'wide2longscript'

    def displayName(self):
        """
        Returns the translated algorithm name, which should be used for any
        user-visible display of the algorithm name.
        """
        return self.tr('Wide2Long Script')

    def group(self):
        """
        Returns the name of the group this algorithm belongs to. This string
        should be localised.
        """
        return self.tr('Wide2Long scripts')

    def groupId(self):
        """
        Returns the unique ID of the group this algorithm belongs to. This
        string should be fixed for the algorithm, and must not be localised.
        The group id should be unique within each provider. Group id should
        contain lowercase alphanumeric characters only and no spaces or other
        formatting characters.
        """
        return 'wide2longscripts'

    def shortHelpString(self):
        """
        Returns a localised short helper string for the algorithm. This string
        should provide a basic description about what the algorithm does and the
        parameters and outputs associated with it..
        """
        return self.tr("Wide2Long algorithm")

    def initAlgorithm(self, config=None):
        """
        Here we define the inputs and output of the algorithm, along
        with some other properties.
        """

        # We add the input vector features source. It can have any kind of
        # geometry.
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.INPUT,
                self.tr('Input layer'),
                [QgsProcessing.TypeVectorAnyGeometry]
            )
        )

        # We add a feature sink in which to store our processed features (this
        # usually takes the form of a newly created vector layer when the
        # algorithm is run in QGIS).
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT,
                self.tr('Output layer')
            )
        )
        
        # select those columns that should be copied (don't forget geometry)
        # base_columns_to_keep = ["SOVEREIGNT", "REGION_UN", "NAME_EN", "UN_code", "geometry" ]
        # BASECOLSKEEP = 'BASECOLSKEEP'
        self.addParameter(
            QgsProcessingParameterField(
                self.BASECOLSKEEP,
                self.tr("Select those columns that should be copied unchanged."),
                None,
                self.INPUT,
                type = QgsProcessingParameterField.Any,
                allowMultiple = True,
                optional = False
            )
        )
        
        # a list, the columns that are currently in "wide" format, that should be transposed into row-wise/per feature long format
        # WIDE2LONGCOLUMNS = 'WIDE2LONGCOLUMNS'
        self.addParameter(
            QgsProcessingParameterField(
                self.WIDE2LONGCOLUMNS,
                self.tr("Select a list, the columns that are currently in 'wide' format, that should be transposed into row-wise/per feature long format."),
                None,
                self.INPUT,
                type = QgsProcessingParameterField.Any,
                allowMultiple = True,
                optional = False
            )
        )
        
        # how to name the attribute that should represent the column title (has the value from the wide column name)
        # wide_to_long_orig_column_name_represent = "year"
        # WIDE2LONGORIGCOLNAMEREPR = 'WIDE2LONGORIGCOLNAMEREPR'
        self.addParameter(
            QgsProcessingParameterString(
                self.WIDE2LONGORIGCOLNAMEREPR,
                self.tr("How to name the attribute that should represent the column title (has the value from the wide column name) ?"),
                optional=False,
                multiLine = False,
                defaultValue = "year"
            )
        )
        
        # how to name the attribute that gets the data value 
        # wide_to_long_transpose_data_column = "population"
        # WIDE2LONGTRANSDATACOL = 'WIDE2LONGTRANSDATACOL'
        self.addParameter(
            QgsProcessingParameterString(
                self.WIDE2LONGTRANSDATACOL,
                self.tr("How to name the attribute that gets the data value ?"),
                optional=False,
                multiLine = False,
                defaultValue = "population"
            )
        )
    
    
    def processAlgorithm(self, parameters, context, feedback):
        """
        Here is where the processing itself takes place.
        """

        # Retrieve the feature source and sink. The 'dest_id' variable is used
        # to uniquely identify the feature sink, and must be included in the
        # dictionary returned by the processAlgorithm function.
        source = self.parameterAsSource(
            parameters,
            self.INPUT,
            context
        )

        # If source was not found, throw an exception to indicate that the algorithm
        # encountered a fatal error. The exception text can be any string, but in this
        # case we use the pre-built invalidSourceError method to return a standard
        # helper text for when a source cannot be evaluated
        if source is None:
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT))

        
        # select those columns that should be copied (don't forget geometry)
        # base_columns_to_keep = ["SOVEREIGNT", "REGION_UN", "NAME_EN", "UN_code", "geometry" ]
        base_columns_to_keep = self.parameterAsFields(
            parameters, self.BASECOLSKEEP, context
        )
        
        # a list, the columns that are currently in "wide" format, that should be transposed into row-wise/per feature long format
        wide_to_long_columns = self.parameterAsFields(
            parameters, self.WIDE2LONGCOLUMNS, context
        )
        
        # how to name the attribute that should represent the column title (has the value from the wide column name)
        # wide_to_long_orig_column_name_represent = "year"
        wide_to_long_orig_column_name_represent = self.parameterAsString(
            parameters, self.WIDE2LONGORIGCOLNAMEREPR, context
        )
        
        # how to name the attribute that gets the data value 
        # wide_to_long_transpose_data_column = "population"
        wide_to_long_transpose_data_column = self.parameterAsString(
            parameters, self.WIDE2LONGTRANSDATACOL, context
        )

        # Send some information to the user
        feedback.pushInfo('Input layer is {}'.format(source.sourceName() ))
        feedback.pushInfo('wide_to_long_orig_column_name_represent is {}'.format(wide_to_long_orig_column_name_represent))
        feedback.pushInfo('wide_to_long_transpose_data_column is {}'.format(wide_to_long_transpose_data_column))
        feedback.pushInfo('CRS is {}'.format(source.sourceCrs().authid()))

        # Compute the number of steps to display within the progress bar and
        # get features from source
        total = 100.0 / source.featureCount() if source.featureCount() else 0
        features = source.getFeatures()
        
        field_definitions = QgsFields()
        feedback.pushInfo("Scanning the fields, first round")
        # initial filtering to get the field definitions
        for current, feature in enumerate(features):
            # Stop the algorithm if cancel button has been clicked
            if feedback.isCanceled():
                break
            
            # scan the first feature
            f_fields = feature.fields()
            
            new_fields_list = []
            for idx, field_temp in enumerate(f_fields):
                if field_temp.name() in base_columns_to_keep:
                    feedback.pushInfo("Keeping as baseinfo for each feature: field name {}, field type {}".format(field_temp.name(), field_temp.typeName()) )
                    new_fields_list.append(field_temp)
            
            long_field_feature_type = None
            # per each wide column that should be transposed into long (multiple features)
            for each_single_column in wide_to_long_columns:
                
                cur_field_idx = f_fields.indexFromName(each_single_column)
                long_field = f_fields.at(cur_field_idx)
                if cur_field_idx >= 0:
                    feedback.pushInfo("field name {}, field type {}".format(each_single_column, long_field.typeName()) )
                else:
                    feedback.pushInfo("field name {} not found?!?!".format(each_single_column))
                    continue
                
                # Only take one wide column at a time and create multiple features
                # take the name of the column as attribute name
                # new_feat[wide_to_long_orig_column_name_represent] = each_single_column
                represent_field = QgsField(wide_to_long_orig_column_name_represent, QVariant.String)
                new_fields_list.append(represent_field)
                
                # take the value of the column for that current feature attribute value
                # new_feat[wide_to_long_transpose_data_column] = feature[each_single_column]
                new_long_field = QgsField(wide_to_long_transpose_data_column, long_field.type())
                new_fields_list.append(new_long_field)
            
            # only do first feature
            for f in new_fields_list:
                field_definitions.append(f)
            break
        
        # we need the new field definitions before creating the sink
        
        # If sink was not created, throw an exception to indicate that the algorithm
        # encountered a fatal error. The exception text can be any string, but in this
        # case we use the pre-built invalidSinkError method to return a standard
        # helper text for when a sink cannot be evaluated
        (sink, dest_id) = self.parameterAsSink(
            parameters,
            self.OUTPUT,
            context,
            
            field_definitions,  # <<<----------- source.fields()
            
            source.wkbType(),
            source.sourceCrs()
        )
        
        if sink is None:
            raise QgsProcessingException(self.invalidSinkError(parameters, self.OUTPUT))
        
        feedback.pushInfo("building the features, second round")
        # per each base feature
        for current, feature in enumerate(features):
            # Stop the algorithm if cancel button has been clicked
            if feedback.isCanceled():
                break
            
            # per each wide column that should be transposed into long (multiple features)
            for each_single_column in wide_to_long_columns:
                
                # new feature, attributes should be known from layer field definitions?
                new_feat = QgsFeature(field_definitions)
                
                # create the "base" feature info
                for keep_column_name in base_columns_to_keep:
                    # set a single attribute by key or by index:
                    val = feature[keep_column_name]
                    new_feat.setAttribute(keep_column_name, val)
                
                # take the value of the column for that current feature attribute value
                # new_feat[wide_to_long_transpose_data_column] = feature[each_single_column]
                new_feat.setAttribute(wide_to_long_orig_column_name_represent, str(each_single_column))
                
                val = feature[each_single_column]
                new_feat.setAttribute(wide_to_long_transpose_data_column, val)
                
                # check the geometry if needed
                new_feat.setGeometry(feature.geometry())
            
                # Add a feature in the sink
                sink.addFeature(new_feat, QgsFeatureSink.FastInsert)

            # Update the progress bar
            feedback.setProgress(int(current * total))

        
        # Return the results of the algorithm. In this case our only result is
        # the feature sink which contains the processed features, but some
        # algorithms may return multiple feature sinks, calculated numeric
        # statistics, etc. These should all be included in the returned
        # dictionary, with keys matching the feature corresponding parameter
        # or output names.
        return {self.OUTPUT: dest_id}
