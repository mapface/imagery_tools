'''
Extracts a list of projects from a public ArcGIS services rest end point.

Built specifically for Qld Dept of Resources imagery services.

Can work with private services if you have a token.

project_list_rest_endpoint.py {service name} {output path} {query} {token (if restricted service)}
if you want the whole project list use 1=1 as the query


Python 3.x+
'''

import requests
import csv
import os
from datetime import datetime
import click


@click.command()
@click.argument("service_name", type=click.Choice(
    ['AerialOrtho_AllUsers',
     'NaturalDisaster_Analytic_AllUsers',
     'NaturalDisaster_Optical_AllUsers', 'EarliestAerialOrtho_AllUsers',
     'LatestSatelliteWOS_AllUsers', 'LatestStateProgram_AllUsers',
     'DEM_TimeSeries_AllUsers']))
@click.argument("outpath", type=click.Path(exists=True), required=False)
@click.argument("selection", type=click.STRING, default="1=1")
@click.argument("agol_token", type=click.STRING, default="")
def main(service_name, outpath, selection, agol_token):
    # variables
    maxrc = 1000  # esri REST api query limit
    projects = []
    project_count = 0

    # determine service type
    basemaps = ['EarliestAerialOrtho_AllUsers', 'LatestSatelliteWOS_AllUsers', 'LatestStateProgram_AllUsers']
    timeseries = ['AerialOrtho_AllUsers', 'NaturalDisaster_Analytic_AllUsers', 'NaturalDisaster_Optical_AllUsers']
    elevation = ['DEM_TimeSeries_AllUsers']

    if service_name in basemaps:
        service_type = 'Basemaps'
    elif service_name in timeseries:
        service_type = 'TimeSeries'
    elif service_name in elevation:
        service_type = 'Elevation'

    # construct url
    prefix = f'https://spatial-img.information.qld.gov.au/'
    service = f'arcgis/rest/services/{service_type}/{service_name}/ImageServer/'
    token = '&token=' + agol_token

    # determine name field
    if agol_token != "":
        url = prefix + service + "?f=pjson" + token
    else:
        url = prefix + service + "?f=pjson"

    fields_json = requests.get(url).json()
    fields = fields_json['fields']

    for f in fields:
        name = f['name']
        if 'Name' in name:
            name_field = 'Name'
        elif 'name' in name:
            name_field = 'name'

    # Queries
    query = f"query?where={selection}&returnGeometry=false&outFields={name_field}&f=json"
    obj_query = f"query?where={selection}&returnIdsOnly=true&f=json"

    # url as string
    if agol_token != "":
        urlstring = prefix + service + obj_query + token
    else:
        urlstring = prefix + service + obj_query

    # identify number of records
    response = requests.get(urlstring)
    js = response.json()
    idfield = js["objectIdFieldName"]
    idlist = js["objectIds"]
    idlist.sort()
    numrec = len(idlist)
    print(f"\nNumber of target records: {numrec}")

    def get_projects(query):
        if agol_token != "":
            url_query = prefix + service + query + token
        else:
            url_query = prefix + service + query

        response_json = requests.get(url_query).json()
        features = response_json['features']

        for f in features:
            name = f['attributes'][name_field]
            projects.append(name)

    # Gather features
    if selection != "1=1":
        get_projects(query)

    else:
        print("\nGathering records...")
        for i in range(0, numrec, maxrc):
            torec = i + (maxrc - 1)
            if torec > numrec:
                torec = numrec - 1
            fromid = idlist[i]
            toid = idlist[torec]
            where = f"{idfield} >= {fromid} and {idfield} <= {toid}"
            fquery = f"query?where={where}&returnGeometry=false&outFields={name_field}&f=json"
            print(f"  {where}")
            get_projects(fquery)

    projects.sort()

    # Add records to CSV
    date_now = datetime.today().strftime('%Y%m%d')

    selection_string = str(selection).replace(" ", "_")
    outfile = (os.path.join(outpath, service_name) + f"_{selection_string}_{date_now}.csv")

    with open(outfile, 'w', newline="") as file:
        wr = csv.writer(file)
        for project in projects:
            project_count += 1
            wr.writerow([project])

    # Fin
    print(f"\nScript finished. Total Projects Added: {project_count}")


if __name__ == "__main__":
    main()
