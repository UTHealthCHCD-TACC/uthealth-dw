# FDA has historical and current ndc through their API
import requests 
import zipfile 
import io
import os


def get_fda_ndc(output_dir, url):
    try:
        os.mkdir(output_dir)
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(output_dir)
        print('Zip Extracted')
    except:
        print('File already exists')

if __name__ == '__main__':

    output_dir = './fda_ndc_output'
    url = 'https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip'
    get_fda_ndc(output_dir, url)