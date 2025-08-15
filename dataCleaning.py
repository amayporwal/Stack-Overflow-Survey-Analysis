import pandas as pd
import numpy as np
import sqlite3


conn = sqlite3.connect('survey.db')



df = pd.read_csv("survey_data.csv")

#remove duplicates
df.drop_duplicates(subset=["ResponseId"], inplace=True)
df.drop_duplicates(inplace=True)

# replacing missing values with most frequent value
df["RemoteWork"]=df["RemoteWork"].fillna(df["RemoteWork"].value_counts().idxmax())

#
df["YearsCode"] = df["YearsCode"].replace({
    "More than 50 years": 50,
    "Less than 1 year": 0
})
df["YearsCode"] = pd.to_numeric(df["YearsCode"])
df.dropna(subset=["YearsCode"], inplace=True)

df["CodingActivities"]=df["CodingActivities"].ffill() #forwardfill
df["ConvertedCompYearly"]=df["ConvertedCompYearly"].fillna(df["ConvertedCompYearly"].mean())

df["Country"] = df["Country"].replace({
    "United States": "US",
    "United States of America": "USA",
    "United Kingdom of Great Britain and Northern Ireland": "UK"})
df["Country"] = df["Country"].fillna(df["Country"].value_counts().idxmax())

df["EdLevel"] = df["EdLevel"].replace({
    "Bachelor’s degree (B.A., B.S., B.Eng., etc.)": "Bachelor’s degree",
    "Master’s degree (M.A., M.S., M.Eng., MBA, etc.)": "Master’s degree",
    "Some college/university study without earning a degree": "No degree",
    "Secondary school (e.g. American high school, German Realschule or Gymnasium, etc.)": "Secondary school",
    "Professional degree (JD, MD, Ph.D, Ed.D, etc.)": "Professional degree",
    "Associate degree (A.A., A.S., etc.)": "Associate degree",
    "Primary/elementary school": "Primary/elementary school",
    "Something else": "Others"})

df["EdLevel"] = df["EdLevel"].fillna("No degree")

df.dropna(subset=["CompTotal"], inplace=True)


for col in df.columns:
    if df[col].isnull().sum() != 0:

        if pd.api.types.is_numeric_dtype(df[col]):  # numeric check
            df[col] = df[col].fillna(df[col].mean())  
        else:
            df[col] = df[col].fillna(df[col].value_counts().idxmax())



# normalizing data 
mean_val = df["ConvertedCompYearly"].mean()
sd_val = df["ConvertedCompYearly"].std()
min_val = df["ConvertedCompYearly"].min()

df["ConvertedCompYearly"] = (df["ConvertedCompYearly"])/(min_val)


#drop columns 

df.drop(columns=["Check","LearnCode","LearnCodeOnline","TechDoc",],inplace=True)


df["YearsCodePro"] = df["YearsCodePro"].replace({
    "More than 50 years": 50,
    "Less than 1 year": 0
})
df["YearsCodePro"] = pd.to_numeric(df["YearsCodePro"])



def experience(years):
    if years < 5:
        return "Beginner"
    elif 5 <= years < 10:
        return "Intermediate"
    elif 10 <= years < 20:
        return "Advanced"
    else:
        return "Expert"

df["ExperienceLevelCoding"] = df["YearsCodePro"].apply(experience)


Time_Transformation = ({
    '30-60 minutes a day' : 45,
    '60-120 minutes a day' : 90 ,
    '15-30 minutes a day' : 22.5,
    'Less than 15 minutes a day' : 7.5,
    'Over 120 minutes a day' : 120
})

df["TimeSearchingTransformed"] = df["TimeSearching"].map(Time_Transformation)
df["TimeAnsweringTransformed"] = df["TimeAnswering"].map(Time_Transformation)

participation = {
    'I have never participated in Q&A on Stack Overflow': 0,
    'Less than once per month or monthly': 1,
    'A few times per month or weekly': 2,
    'A few times per week': 3,
    'Daily or almost daily': 4,
    'Multiple times per day': 5
}

df["Participation_Freq_Scale"] = df["SOPartFreq"].map(participation)

age_numeric = ({
    "25-34 years old" : 30,
    "35-44 years old" : 40,
    "18-24 years old"  : 20,
    "45-54 years old"  : 50,
    "55-64 years old"   : 60,
    "Under 18 years old" :  18,
    "65 years or older": 65,
    "Prefer not to say": 0
})



df["Age_transformed"] = df["Age"].map(age_numeric)
df = df[df["Age_transformed"] != 0]

unused_columns = [
    "Check",  "TechDoc",
    "PurchaseInfluence", "BuyNewTool", "BuildvsBuy", "TechEndorse",
    "MiscTechHaveWorkedWith", "MiscTechWantToWorkWith", "MiscTechAdmired",
    "ToolsTechHaveWorkedWith", "ToolsTechWantToWorkWith", "ToolsTechAdmired",
    "OfficeStackAsyncHaveWorkedWith", "OfficeStackAsyncWantToWorkWith", "OfficeStackAsyncAdmired",
    "OfficeStackSyncHaveWorkedWith", "OfficeStackSyncWantToWorkWith", "OfficeStackSyncAdmired",
    "AISearchDevHaveWorkedWith", "AISearchDevWantToWorkWith", "AISearchDevAdmired",
    "NEWSOSites", "SOVisitFreq", "SOAccount", "SOHow", "SOComm", "AISelect", "AISent", "AIBen",
    "AIAcc", "AIComplex",
    "AIToolNot interested in Using", "AINextMuch more integrated", "AINextNo change",
    "AINextMore integrated", "AINextLess integrated", "AINextMuch less integrated", "AIThreat",
    "AIEthics", "AIChallenges", "TBranch", "ICorPM",
    "Knowledge_2", "Knowledge_3", "Knowledge_4", "Knowledge_5", "Knowledge_6",
    "Knowledge_7", "Knowledge_8", "Knowledge_9",
    "Frequency_1", "Frequency_2", "Frequency_3",
    "ProfessionalTech", "ProfessionalCloud", "ProfessionalQuestion", "JobSatPoints_4", "JobSatPoints_5",
    "JobSatPoints_8", "JobSatPoints_9", "JobSatPoints_10", "JobSatPoints_11",
    "SurveyLength", "SurveyEase"
]

df = df.drop(columns=unused_columns, errors="ignore")

print(df.shape)
df.to_sql('survey_data', conn, if_exists='replace', index=False)
conn.close()
