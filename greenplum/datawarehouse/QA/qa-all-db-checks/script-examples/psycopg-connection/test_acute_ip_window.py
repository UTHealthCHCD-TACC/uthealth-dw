"""
Created on Tue Nov  9 15:30:07 2021

@author: gmunoz
"""
import pandas as pd
import unittest
import dw_ip_window_step_2 as iw


class MyTest(unittest.TestCase):
    
    def setUp(self):
        self.df = pd.read_csv('test_ip_window_data.csv',
                              dtype={'discharge_status':'str'}, 
                              parse_dates=['admit_date','discharge_date'])
        self.df['discharge_status'].fillna('NA', inplace=True)

    def test_discharge_adjust(self):
        df = self.df.copy(deep=True)
        df = iw.transfer_dt_adjuster(df)
        self.assertTrue(df.loc[0, 'transfer_cd'])
        self.assertEqual(df.loc[0,'transfer_adj_discharge_date'], pd.to_datetime('2018-01-02'))
        self.assertFalse(df.loc[1, 'transfer_cd'])
        
    def test_encounter_identifier(self):
        df = self.df.copy(deep=True)
        df = iw.transfer_dt_adjuster(df)
        df = iw.enc_identifier(df, is_sorted=False)
        #first two should be the first encounter
        self.assertEqual(df.loc[0,'enc_id'],0)
        self.assertEqual(df.loc[1,'enc_id'],0)
        self.assertEqual(df.loc[2,'enc_id'],1)
        self.assertEqual(df.loc[0,'enc_admit_date'], pd.to_datetime('2018-01-01'))
        self.assertEqual(df.loc[0,'enc_discharge_date'], pd.to_datetime('2018-01-03'))
        
        max_encounter = df.loc[df['uth_member_id']==1, 'enc_id'].max()
        self.assertEqual(max_encounter, 3)
        
    def test_admit_it_output(self):
        df = self.df.copy(deep=True)
        df = iw.transfer_dt_adjuster(df)
        df = iw.enc_identifier(df, is_sorted=False)
        df.loc[:, 'admit_id'] = iw.admit_id_output(df)
        self.assertEqual(df.loc[14,'admit_id'],'3-002-2018')
        
    def test_encounter_row_counter(self):
        df = self.df.copy(deep=True)
        df = iw.transfer_dt_adjuster(df)
        df = iw.enc_identifier(df, is_sorted=False)
        df = df.set_index(['uth_member_id','enc_id'])
        df = iw.encounter_row_counter(df)

        enc_row_count = df.loc[(1,0), 'enc_row_count']
        self.assertTrue((enc_row_count==2).all())
        enc_row_count = df.loc[(1,1), 'enc_row_count']
        self.assertEqual(len(enc_row_count), 1)
        self.assertEqual(enc_row_count.values[0], 1)
        
    def test_admit_encounter_status(self):
        df = self.df.copy(deep=True)
        df = iw.transfer_dt_adjuster(df)
        df = iw.enc_identifier(df, is_sorted=False)
        df = df.set_index(['uth_member_id','enc_id'])
        df = iw.encounter_row_counter(df)
        encounter_discharge_status = iw.admit_encounter_status(df)
        encounter_discharge_status = encounter_discharge_status.sort_index()
        status = encounter_discharge_status.loc[(1,0), 'enc_discharge_status']
        self.assertEqual(status, '01')
        status = encounter_discharge_status.loc[(2,0), 'enc_discharge_status']
        self.assertEqual(status, '01')
        status = encounter_discharge_status.loc[(2,2), 'enc_discharge_status']
        self.assertEqual(status, '00')
        status = encounter_discharge_status.loc[(3,0), 'enc_discharge_status']
        self.assertEqual(status, '01')
        
if __name__ == '__main__':
    unittest.main()