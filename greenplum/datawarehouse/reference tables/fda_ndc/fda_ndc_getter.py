# FDA has historical and current ndc through their API
import sys
sys.path.append('H:/uth_helpers/')
from uth_helpers import helpers

if __name__ == '__main__':

    output_dir = './fda_ndc_output_test'
    url = 'https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip'
    
    helpers.url_zip_outputter(output_dir, url)
