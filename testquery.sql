-- sample data from : https://github.com/datamyselfai/lakeflow-connect-salesforce-ingestion-data/tree/dea30a21c6c513f6b399275e0024a1129fb04524/salesforce_data
-- update
select Name, stageName from opportunity
where Name = 'DataFlux Expansion 543' 

-- del
select * from contact
where firstname = 'Tina'
and lastname = 'Zimmermann'
and phone = '(842) 576-6344'

-- insert
-- account inserted check
