import pandas as pd

def change_region(region):
    if region == 'NORTH AMERICA':
        return 1
    elif region == 'EUROPE':
        return 2
    elif region == 'ASIA':
        return 3
    elif region == 'SOUTH AMERICA':
        return 4    
    elif region == 'AFRICA':
        return 5
    elif region == 'OCEANIA':
        return 6
    else:
        return 0
    
def main():
    df = pd.read_csv('BreastCancerDatasetClean.csv',delimiter=',')
    dim_person = df[['ACCESS_TO_CARE','EDUCATION_LEVEL']]        
    dim_region = df[['REGION','URBANIZATION_RATE','GDP_PER_CAPITA']]
    
    dim_region['REGION'] = dim_region['REGION'].apply(change_region)
    
    dim_population = df[['HEALTHCARE_EXPENDITURE','SURVIVAL_RATE','BREAST_CANCER_CASES','BREAST_CANCER_DEATHS']]    
    dim_region.to_csv('Region.csv')    
    
if __name__ == '__main__':
    main()