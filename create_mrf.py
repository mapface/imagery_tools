'''
Script to batch convert imagery files to MRF format and upload them to an Amazon S3 bucket.

Requires a config json file.

Intentionally obfuscated.

Python 3
'''

import json
import os
from pathlib import Path
import click
import xml.etree.ElementTree as ET
import subprocess
from osgeo import gdal


@click.command()
@click.argument("project_type", type=click.Choice(
    ["imagery-ortho", "imagery-ortho-rgbi", "jp2-ortho", "ecw-ortho", "tif-dem", "custom"]))
@click.argument("project_path", type=click.Path(exists=True))
@click.option("-c", "--config", type=click.Path(exists=True), default=None)
@click.option("-t", "--test", is_flag=True, default=False, help="Upload to test bucket")
@click.option("-o", "--option", type=click.Choice(["mrf", "proxy", "count"]), default=None, help="")
def main(project_type, project_path, config, test, option):
    with ('services-config.json') as path:
        default_path = path
    config_path = config if config else default_path
    cf = load_json(config_path, project_type)
    if option is "mrf" or option is None:
        create_mrfs(project_path, cf)
        upload_mrfs(project_path, cf, test)
    if option is "proxy" or option is None:
        create_mrf_proxies(project_path, cf, test)
    if option is "count" or option is None:
        check_counts(project_path, cf)


# load config file
def load_json(config, project_type):
    json_read = json.load(open(config))
    return json_read[project_type]


# create mrfs
def create_mrfs(project_path, cf):
    source_path = Path(project_path) / cf['source_folder']

    dest_path = Path(project_path) / cf['mrf_folder']
    dest_path.mkdir(parents=True, exist_ok=True)

    source_rasters = [f for f in source_path.glob(f"**/*{cf['input_ext']}")]

    gdal_output = (cf['output_ext'].replace(".", "")).upper()

    print(f"\n{len(source_rasters)} rasters identified")

    for raster in source_rasters:
        dest_raster_path = dest_path / (raster.stem + cf['output_ext'])

        print(f"\nCreating {dest_raster_path}")

        raster_proj = subprocess.check_output(['gdalsrsinfo', '-o', 'wkt_esri', '--single-line', raster]).decode(
            'utf-8')

        # translate
        translate_options = gdal.TranslateOptions(gdal.ParseCommandLine(
            f"-of {gdal_output} -co COMPRESS={cf['compression']} -co QUALITY=75 -co BLOCKSIZE=512 -a_srs \"{raster_proj}\""))
        gdal.Translate(str(dest_raster_path), str(raster), options=translate_options)

        # addo
        image = gdal.Open(str(dest_raster_path), 1)
        gdal.SetConfigOption('COMPRESS_OVERVIEW LERC', 'DEFLATE')
        image.BuildOverviews('AVERAGE', [2], gdal.TermProgress_nocb)
        del image


def upload_mrfs(project_path, cf, test):
    directory = Path(project_path)
    source_directory = directory / cf['mrf_folder']
    project_folder = directory.name

    if test is True:
        destination_path = f"s3://{cf['bucket']}/{cf['directory']}-test/{project_folder}/{cf['mrf_folder']}"
    else:
        destination_path = f"s3://{cf['bucket']}/{cf['directory']}/{project_folder}/{cf['mrf_folder']}"

    cmd = f"aws s3 sync --acl {cf['acl']} {source_directory} {destination_path}"
    print("\nCopying MRF files to S3")
    subprocess.call(cmd, shell=True)


def create_mrf_proxies(project_path, cf, test):
    directory = Path(project_path)
    project_folder = directory.name

    mrf_proxy_path = directory / cf['proxy_folder']
    mrf_proxy_path.mkdir(parents=True, exist_ok=True)

    gdal_output = (cf['output_ext'].replace(".", "")).upper()

    if test is True:
        mrf_directory_path = f"s3://{cf['bucket']}/{cf['directory']}-test/{project_folder}/{cf['mrf_folder']}/"
    else:
        mrf_directory_path = f"s3://{cf['bucket']}/{cf['directory']}/{project_folder}/{cf['mrf_folder']}/"

    aws_ls = os.popen(f"aws s3 ls {mrf_directory_path}").read()
    rows = aws_ls.strip().split("\n")
    mrf_files = [r.split()[-1] for r in rows]
    mrf_files = [r for r in mrf_files if r.endswith(cf['output_ext'])]

    for f in mrf_files:
        if test is True:
            mrf_path = f"/vsicurl/http://amazonaws.com/{cf['bucket']}/{cf['directory']}-test/{project_folder}/{cf['mrf_folder']}/{f}"
        else:
            mrf_path = f"/vsicurl/http://amazonaws.com/{cf['bucket']}/{cf['directory']}/{project_folder}/{cf['mrf_folder']}/{f}"

        mrf_proxy = mrf_proxy_path / f

        # translate
        translate_options = gdal.TranslateOptions(gdal.ParseCommandLine(
            f"-of {gdal_output} -co COMPRESS={cf['compression']} -co QUALITY=75 -co NOCOPY=True -co CACHEDSOURCE={mrf_path} -co UNIFORM_SCALE=2"))
        gdal.Translate(str(mrf_proxy), str(mrf_path), options=translate_options)

        print(f"\nCreating MRF Proxies - {f}")

        mrf_xml = ET.parse(mrf_proxy)
        root = mrf_xml.getroot()
        raster_element = root.find("Raster")

        cache_file = f.replace(cf['output_ext'], cf['cache_ext'])
        index_file = f.replace(cf['output_ext'], cf['mrf_index'])

        cache_file_path = Path(cf['cache_path']) / project_folder / cf['cache_folder'] / cache_file

        data_file_element = ET.SubElement(raster_element, "DataFile")
        data_file_element.text = str(cache_file_path)

        index_file_element = ET.SubElement(raster_element, "IndexFile")
        index_file_element.text = str(cache_file_path)

        mrf_xml.write(mrf_proxy)


def check_counts(project_path, cf):
    # paths
    directory = Path(project_path)
    source_dir = directory / cf['source_folder']
    proxy_dir = directory / cf['proxy_folder']

    # lists
    source_list = [f.stem for f in source_dir.glob(f"**/*{cf['input_ext']}")]
    proxy_list = [f.stem for f in proxy_dir.glob(f"**/*{cf['output_ext']}")]

    print(f"\nTotal {cf['input_ext']}: {len(source_list)}")
    print(f"\nTotal {cf['output_ext']}: {len(proxy_list)}")

    if len(source_list) != len(proxy_list):
        print("\nDifference: ", len(source_list) - len(proxy_list))

        for i in list(source_list):
            if i.strip("',") in proxy_list:
                source_list.remove(i)

        # Print Missing
        print("Missing Proxies: ", source_list)
    else:
        print("No Missing Proxies Detected.")

    print("\nScript Complete.")


if __name__ == "__main__":
    main()
