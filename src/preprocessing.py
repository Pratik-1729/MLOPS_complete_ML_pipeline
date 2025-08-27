import os
import logging
from sklearn.preprocessing import LabelEncoder
import pandas as pd
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
import string
import nltk
nltk.download('stopwords')
nltk.download('punkt')

log_dir = 'logs'
os.makedirs(log_dir,exist_ok=True)

logger = logging.getLogger("Pre-processing")
logger.setLevel('DEBUG')

console_handler = logging.StreamHandler()
console_handler.setLevel('DEBUG')

log_file_path = os.path.join(log_dir,'data_preprocessing.log')
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel('DEBUG')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

def transform_text(text):
    """Transforming the text by converting it into lowercase, tokenizing, removing  the stop words 
    and punctuations , and stemming"""

    ps = PorterStemmer()
    text = text.lower() #lower the lext

    text = nltk.word_tokenize(text) #tokenizing the text

    text = [word for word in text if word.isalnum()] #remove non alpha numeric text

    text = [word for word in text if word not in stopwords.words('english') and word not in string.punctuation] #remove stopwords and punctuations
    
    text = [ps.stem(word) for word in text] #stem the words

    return " ".join(text) #join the tokens into a single string

def preprocess_df(df, text_column = 'text', target_column = 'target'):
    """
    Preprocess the df by encoding the target column, removing duplicates and transforming the text 
    column"""
    try:
        logger.debug('Starting the preprocessing for DataFrame')
        #encode the target column
        encoder = LabelEncoder()
        df[target_column] = encoder.fit_transform(df[target_column])
        logger.debug('Target column encoded')

        # remove duplicate rows
        df = df.drop_duplicates(keep='first')
        logger.debug('removed duplicates')

        # apply transformation to the specified text column
        df.loc[:,text_column] = df[text_column].apply(transform_text)
        logger.debug('text column transformed')
        return df
    except KeyError as e:
        logger.error('Column not found: %s',e)
        raise
    except Exception as e:
        logger.error('Error occurred during text normalization: %s',e)
        raise


def main(text_column ='text',target_column = 'target'):
    """
    Main function to load the raw data, preprocess it and save the processed data
    """
    try:
        # fetch the data from data/raw
        train_data = pd.read_csv('./data/raw/train.csv')
        test_data = pd.read_csv('./data/raw/test.csv')
        logger.debug('data loaded successfully')

        # transform the data
        train_processed_data = preprocess_df(train_data, text_column, target_column)
        test_processed_data = preprocess_df(test_data,text_column,target_column)

        # store the data in data/processed
        data_path = os.path.join('./data','interim')
        os.makedirs(data_path,exist_ok=True)

        train_processed_data.to_csv(os.path.join(data_path,'train_preprocessed.csv'),index = False)
        test_processed_data.to_csv(os.path.join(data_path,'test_preprocessed.csv'),index=False)

        logger.debug('processed data saved to %s',data_path)
    except FileNotFoundError as e:
        logger.error('File not found %s',e)
    except pd.errors.EmptyDataError as e:
        logger.error('No data %s',e)
    except Exception as e:
        logger.error('Failed to complete the data transformation process %s',e)
        print(f"error{e}")

if __name__ == '__main__':
    main()

    





