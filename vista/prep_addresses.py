"""Utilities to prepare VISTA addresses for cloud geocoding."""
import arcpy
import os
import sys
import csv

query = """
SELECT
ar.RESIDENCE_ID as UNIQUE_ID,
rownum as ROW_ID,
ar.RESIDENCE_ID,
ar.ADDRESS  as VISTA_ADDRESS,
ar.CITY as VISTA_CITY,
ar.ZIP as VISTA_ZIP,
ar.X as VISTA_X,
ar.Y as VISTA_Y,
p.PRECINCT as VISTA_PRECINCT,
ar.COUNTY_ID as CountyID
FROM GV_VISTA.AGRC_RESIDENCE ar INNER JOIN GV_VISTA.PRECINCTS p
ON
ar.PRECINCT_ID = p.PRECINCT_ID
"""

partition_field = 'partition'


def partition_table(path, parts, county_ids=[]):
    """Add partition field and assign partition number."""
    county_where = None
    if len(county_ids) > 0:
        county_where = 'COUNTYID in ({})'.format(','.join(str(x) for x in county_ids))
    arcpy.AddField_management(path, partition_field, 'SHORT')
    row_count = 0
    with arcpy.da.UpdateCursor(path, partition_field, county_where) as cursor:
        for row in cursor:
            row[0] = row_count % parts
            cursor.updateRow(row)
            row_count += 1


def separate_partitions(parts_table_path, parts, workspace, csv_folder):
    """Separate partions into new CSVs."""
    tables = []
    for i in range(parts):
        layer_name = 'addr_part_{}'.format(i)
        part_where = '{} = {}'.format(partition_field, i)
        arcpy.MakeTableView_management(parts_table_path, layer_name, part_where)
        tables.append(arcpy.CopyRows_management(layer_name, os.path.join(workspace, layer_name))[0])
    for t in tables:
        with arcpy.da.SearchCursor(t, '*') as cursor,\
            open(os.path.join(csv_folder, os.path.basename(t) + '.csv'), 'w') as out_csv:
            csv_writer = csv.writer(out_csv)
            fields = cursor.fields
            csv_writer.writerow(fields)
            for row in cursor:
                csv_writer.writerow(row)



if __name__ == '__main__':
    vista_database_connection = r'C:\Users\kwalker\AppData\Roaming\ESRI\Desktop10.4\ArcCatalog\Vista - Connection to slxp3.dts.utah.gov.sde'
    output_folder = r'C:\giswork\vista\address_check2018\Counties_vista\Salt_Lake\oct_24_2018'
    output_gdb = 'Vista_InputData.gdb'
    output_workspace = os.path.join(output_folder, output_gdb)
    output_vista_data_table = 'SLCO_Vista_Addresses'
    csv_folder = os.path.join(output_folder, 'job_uploads')

    if not arcpy.Exists(output_workspace):
        arcpy.CreateFileGDB_management(output_folder, output_gdb)
    else:
        print('output_gdb exists')
    if not os.path.exists(csv_folder):
        os.mkdir(csv_folder)
    else:
        print('csv_folder exists')

    vist_ql = arcpy.MakeQueryLayer_management(input_database=vista_database_connection,
                                              out_layer_name="VDATA",
                                              query=query,
                                              oid_fields="UNIQUE_ID",
                                              shape_type="", srid="", spatial_reference="")

    vista_tableview = arcpy.mapping.TableView(vist_ql[0])
    row_count = int(arcpy.GetCount_management(vista_tableview).getOutput(0))
    print('total rows:', row_count)
    address_table = arcpy.TableToTable_conversion(vista_tableview,
                                                  output_workspace,
                                                  output_vista_data_table)[0]
    partition_table(address_table, 3, [18])
    print('Data created:', address_table)
    separate_partitions(address_table, 3, output_workspace, csv_folder)
