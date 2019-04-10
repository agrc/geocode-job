"""Combine geocoding results into an ArcGIS table"""
import arcpy
import os
import math

ADD_RESULT_FIELDS = (
    ('AGRC_MatchAddress', 'TEXT', 200),
    ('AGRC_Zone', 'TEXT', 50),
    ('AGRC_MatchScore', 'FLOAT', None),
    ('AGRC_X', 'DOUBLE', None),
    ('AGRC_Y', 'DOUBLE', None),
    ('AGRC_Geocoder', 'TEXT', 50),
    ('AGRC_Precinct', 'TEXT', 25),
    ('Distance_Meters', 'DOUBLE', None)
)

GEOCODE_FIELDS = (
    'MatchAddress',
    'Zone_',
    'Score',
    'XCoord',
    'YCoord',
    'Geocoder',
    'VistaID',
)

EXCLUDE_FIELDS = ['Distance_Meters']
RESULT_GEOCODE_MAP = dict(zip([f[0] for f in ADD_RESULT_FIELDS if f[0] not in EXCLUDE_FIELDS],
                              GEOCODE_FIELDS))


def _add_agrc_fields(data_path):
    for field_info in ADD_RESULT_FIELDS:
        field_name, field_type, field_length = field_info
        arcpy.AddField_management(data_path,
                                  field_name=field_name,
                                  field_type=field_type,
                                  field_length=field_length)


def join_geocode_attributes(vista_input, geocode_results):
    # _add_agrc_fields(vista_input)
    where = 'partition IS NOT NULL'
    vista_layer = arcpy.MakeTableView_management(vista_input, 'vista', where)[0].name
    #vista_insert_table = arcpy.CopyRows_management(vista_layer, os.path.join(r'C:\giswork\vista\address_check2018\Counties_vista\Salt_Lake\working_data.gdb', vista_layer + 'again'))[0]
    extra_vista_fields = ['VISTA_X', 'VISTA_Y']
    extra_vista_fields = ['Vista_SLCO_Test.' + f for f in extra_vista_fields]
    geocode_layer = arcpy.MakeTableView_management(geocode_results, 'geocode')[0].name
    result_fields = ['Vista_SLCO_Test.' + f[0] for f in ADD_RESULT_FIELDS] +\
                    ['Geocode_W_Precincts_test.' + f for f in GEOCODE_FIELDS]
    result_fields.extend(extra_vista_fields)
    pos = result_fields.index
    arcpy.AddJoin_management(vista_layer, 'RESIDENCE_ID', geocode_layer, 'INID')
    # ArcGIS joins suck badly
    # use a search and insert cursor. keep field order the same
    with arcpy.da.SearchCursor(vista_layer, result_fields) as cursor:
        for row in cursor:
            import pdb; pdb.set_trace()
            for r_field, g_field in RESULT_GEOCODE_MAP.items():
                r_field = 'Vista_SLCO_Test.' + r_field
                g_field = 'Geocode_W_Precincts_test.' + g_field
                row[pos(r_field)] = row[pos(g_field)]
            import pdb; pdb.set_trace()

            # vista_x = row[pos('VISTA_X')]
            # vista_y = row[pos('VISTA_Y')]
            # agrc_x = row[pos('AGRC_X')]
            # agrc_y = row[pos('AGRC_Y')]
            # row[pos('Distance_Meters')] = math.hypot(agrc_x - vista_x, agrc_y - vista_y)


def dist(vista_x, vista_y, agrc_x, agrc_y):
    import math
    if not vista_x or not vista_y or not agrc_x or not agrc_y:
        return None
    elif vista_x > 0 or vista_y > 0 or agrc_x > 0 or agrc_y > 0:
        return math.hypot(agrc_x - vista_x, agrc_y - vista_y)
    else:
        return None


if __name__ == '__main__':
    vista_data = r'C:\giswork\vista\counties\salt lake\oct_24_2018\Vista_InputData.gdb\SLCO_Only_VISTA'
    geocode_data = r'C:\giswork\vista\address_check2018\Counties_vista\Salt_Lake\working_data.gdb\Geocode_W_Precincts_test'
    # Create new table with vista geocode results fields
    _add_agrc_fields(vista_data)
    # copy all results table to new table
    arcpy.management.CopyRows("result_0", r"C:\giswork\vista\counties\salt lake\oct_24_2018\results\results.gdb\all_results", None)
    arcpy.management.Append("result_1;result_2", "all_results", "TEST", None, None, None)
    # run XYTableToPoint_management (in_table, out_feature_class, x_field, y_field, {z_field}, {coordinate_system})
    arcpy.management.XYTableToPoint("all_results", r"C:\Users\kwalker\Documents\ArcGIS\Projects\vista\vista.gdb\all_results_XYTableToPoint", "XCoord", "YCoord", None, "PROJCS['NAD_1983_UTM_Zone_12N',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Transverse_Mercator'],PARAMETER['False_Easting',500000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-111.0],PARAMETER['Scale_Factor',0.9996],PARAMETER['Latitude_Of_Origin',0.0],UNIT['Meter',1.0]];-5120900 -9998100 10000;-100000 10000;-100000 10000;0.001;0.001;0.001;IsHighPrecision")
    # run identity with points and vista precincts
    arcpy.analysis.Identity("all_results_XYTableToPoint", "SGID10.POLITICAL.VistaBallotAreas", r"C:\giswork\vista\counties\salt lake\oct_24_2018\results\results.gdb\geocode_precinct", "ALL", None, "NO_RELATIONSHIPS")
   # join field all geocode fields and vistaid to vista data
    # arcpy.management.CalculateField("SLCO_Only_VISTA", "SLCO_Only_VISTA.AGRC_Zone", "!geocode_precinct.Zone!", "PYTHON3", None)
    # arcpy.management.CalculateField("SLCO_Only_VISTA", "SLCO_Only_VISTA.AGRC_MatchScore", "!geocode_precinct.Score!", "PYTHON3", None)
    # arcpy.management.CalculateField("SLCO_Only_VISTA", "SLCO_Only_VISTA.AGRC_X", "!geocode_precinct.XCoord!", "PYTHON3", None)
    # arcpy.management.CalculateField("SLCO_Only_VISTA", "SLCO_Only_VISTA.AGRC_Y", "!geocode_precinct.YCoord!", "PYTHON3", None)
    # arcpy.management.CalculateField("SLCO_Only_VISTA", "SLCO_Only_VISTA.AGRC_Geocoder", "!geocode_precinct.Geocoder!", "PYTHON3", None)
    # arcpy.management.CalculateField("SLCO_Only_VISTA", "SLCO_Only_VISTA.AGRC_Precinct", "!geocode_precinct.VistaID!", "PYTHON3", None)
    # arcpy.management.CalculateField("SLCO_Only_VISTA", "SLCO_Only_VISTA.Distance_Meters", "dist(!SLCO_Only_VISTA.VISTA_X!,!SLCO_Only_VISTA.VISTA_Y!, !SLCO_Only_VISTA.AGRC_X!,!SLCO_Only_VISTA.AGRC_Y!)", "PYTHON3", r"def dist(vista_x, vista_y, agrc_x, agrc_y):\n    import math\n    if not vista_x or not vista_y or not agrc_x or not agrc_y:\n        return None\n    elif vista_x > 0 or vista_y > 0 or agrc_x > 0 or agrc_y > 0:\n        return math.hypot(agrc_x - vista_x, agrc_y - vista_y)\n    else:\n        return None")
    join_geocode_attributes(vista_data, geocode_data)
   # TODO delete partition field
