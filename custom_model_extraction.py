import os
import re
import calendar
from datetime import datetime
from configobj import ConfigObj
from dateutil.relativedelta import relativedelta
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient


def call_api(path, model_id):
    
    currentdir =  os.path.abspath( os.path.join(os.path.dirname(__file__), '..','..'))
    
    config = ConfigObj(os.path.join(currentdir, 'instance','config.ini')).dict()
    
    endpoint = config["azureai"]["endpoint"] 
    api_key = config["azureai"]["api_key"]
    
    document_analysis_client = DocumentAnalysisClient(
            endpoint=endpoint, credential=AzureKeyCredential(api_key)
        )

    # Load and analyze the document
    with open(path, "rb") as document_file:
        poller = document_analysis_client.begin_analyze_document(
            model_id=model_id, document=document_file
        )
        result = poller.result()   
        
    return result


def convert_to_standard_date(date_string):
    """
    Convert a date string in various formats to 'YYYY-MM-DD' format.
    """
    date_formats = [
        "%d-%m-%Y",         # e.g., 25-12-2024
        "%d-%B-%Y",         # e.g., 25-December-2024
        "%d.%m.%Y",         # e.g., 25.12.2024
        "%d/%m/%Y",         # e.g., 25/12/2024
        "%m/%d/%Y",         # e.g., 12/25/2024
        "%d-%b-%Y",         # e.g., 29-JUN-2024 (newly added format for abbreviated month name)
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_string, fmt)
            return parsed_date.strftime("%Y-%m-%d")  # Convert to standard format
        except ValueError:
            continue
    
    raise ValueError(f"Date format of '{date_string}' is not supported.")


def standardize_bill_month(bill_month):
    """
    Converts various bill month formats into 'MON-YYYY' format.
    """
    # Dictionary to map month abbreviations and names to three-letter format
    month_mapping = {
        "jan": "JAN", "feb": "FEB", "mar": "MAR", "apr": "APR", 
        "may": "MAY", "jun": "JUN", "jul": "JUL", "aug": "AUG", 
        "sep": "SEP", "oct": "OCT", "nov": "NOV", "dec": "DEC"
    }

    try:
        # Step 1: Normalize input (strip spaces and convert to lowercase)
        cleaned = bill_month.strip().lower()

        # Step 2: Remove unexpected characters (e.g., ':', leading dashes, etc.)
        cleaned = re.sub(r"^[^\w]*", "", cleaned)  # Remove leading non-alphanumeric characters
        cleaned = re.sub(r"[^\w]", "-", cleaned)   # Replace non-alphanumeric separators with "-"

        # Step 3: Extract parts of the bill month
        match = re.match(r"([a-z]+)-?(\d{4})", cleaned)
        if match:
            month_str, year = match.groups()
            # Standardize month to three-letter format
            month = month_mapping.get(month_str[:3], None)
            if month:
                return f"{month}-{year}"

        # Step 4: Handle invalid formats
        raise ValueError(f"Invalid bill month format: {bill_month}")
    except Exception as e:
        return f"Error: {e}"


def clean_and_convert_to_float(number_string):
    """
    Cleans and converts a given string to a float with two decimal places.
    Removes unwanted characters and handles various formatting issues.
    Returns the first valid numeric value found.
    """
    if number_string is not None:
        try: 
            
            number_string = number_string.replace(',','').replace(" -", "-").replace("- ", "-").strip()
                        
            numbers = re.findall(r'-?\d+\.\d+|-?\d+', number_string)
                        
            if len(numbers) > 1:
                return float(numbers[0])  # Return only the first value
                        
            if numbers:
                cleaned = numbers[0]
            else:
                return None
            
            # Handle spaces in negative numbers (e.g., "- 51217.00" -> "-51217.00")                        
            cleaned = re.sub(r'[^\d.-]', '', cleaned)            
            
            # Remove multiple dots (e.g., ".99" -> "0.99", "34737.00000" -> "34737.00")
            cleaned = re.sub(r'(?<!\d)\.(?!\d)', '0.', cleaned)
                        
            num = float(cleaned)
                        
            return num
        except ValueError:
            return "Invalid number"
        except Exception as e:
            return f"Error: {e}"
    else:
        return None


def get_data(path, modelid):
    
    result = call_api(path, modelid)
    
    # Initialize the dictionary and updated dictionary
    extracted_dict = {name: field.value for document in result.documents for name, field in document.fields.items()}

    # Preprocess specific fields and add them to the updated dictionary
    merged_dict = {
        'billdate': convert_to_standard_date(extracted_dict['billdate']),
        'billmonth': standardize_bill_month(extracted_dict['billmonth'])
    }

    # Define a set for excluded keys for faster membership checks
    excluded_keys = {'billdate', 'billmonth'}

    # Process and add remaining fields
    merged_dict.update({
        key: clean_and_convert_to_float(value)
        for key, value in extracted_dict.items()
        if key not in excluded_keys
    })

    total_consumed_units = 0.0
    
    if merged_dict["kvahconsumptionindustrial"]:
        total_consumed_units = merged_dict["kvahconsumptionindustrial"]
    elif merged_dict["kwhconsumptionindustrial"]:
        total_consumed_units = merged_dict["kwhconsumptionindustrial"]
    elif merged_dict["totalconsumptionkvah"]:
        total_consumed_units = merged_dict["totalconsumptionkvah"]
    elif merged_dict["totalconsumptionkwh"]:
        total_consumed_units = merged_dict["totalconsumptionkwh"]


    commercials = {
        "industrialconsumptioncharge": merged_dict.get("industrialconsumptioncharge") or 0.00,
        "commercialconsumptioncharge": merged_dict.get("commercialconsumptioncharge") or 0.00,
        "residentialconsumptioncharge": merged_dict.get("residentialconsumptioncharge") or 0.00,
        "totalenergyconsumptioncharge": (merged_dict.get("industrialconsumptioncharge") or 0.00)
                                        + (merged_dict.get("commercialconsumptioncharge") or 0.00)
                                        + (merged_dict.get("residentialconsumptioncharge") or 0.00),
        "demandcharges": merged_dict.get("demandcharges") or 0.00,
        "wheelingcharges": merged_dict.get("wheelingcharges") or 0.00,
        "faccharge": merged_dict.get("faccharge") or 0.00,
        "todchargeszone1": merged_dict.get("todchargeszone1") or 0.00,
        "todchargeszone2": merged_dict.get("todchargeszone2") or 0.00,
        "todchargeszone3": merged_dict.get("todchargeszone3") or 0.00,
        "todchargeszone4": merged_dict.get("todchargeszone4") or 0.00,
        "pfrebate": merged_dict.get("pfrebate") or 0.00,
        "electricityduty": merged_dict.get("electricityduty") or 0.00,
        "bulkconsumptionrebate": merged_dict.get("bulkconsumptionrebate") or 0.00,
        "incrementalconsumptionrebate": merged_dict.get("incrementalconsumptionrebate") or 0.00,
        "demandpenalty": merged_dict.get("demandpenalty") or 0.00,
        "taxonsale": merged_dict.get("taxonsale") or 0.00,
        "tcs": merged_dict.get("tcs") or 0.00,
        "totalbillamount": merged_dict.get("totalbillamount") or 0.00
    }

    consumptioninformation = {
        "kwhcurrentindustrial": merged_dict.get("kwhcurrentindustrial") or 0.00,
        "kwhpreviousindustrial": merged_dict.get("kwhpreviousindustrial") or 0.00,
        "kvahcurrentindustrial": merged_dict.get("kvahcurrentindustrial") or 0.00,
        "kvahpreviousindustrial": merged_dict.get("kvahpreviousindustrial") or 0.00,
        "multiplicationfactor": merged_dict.get("multiplicationfactor") or 0.00,
        "adjustmentunitsindustrialkwh": merged_dict.get("adjustmentunitsindustrialkwh") or 0.00,
        "adjustmentunitsindustrialkvah": merged_dict.get("adjustmentunitsindustrialkvah") or 0.00,
        "kwhconsumptionindustrial": merged_dict.get("kwhconsumptionindustrial") or 0.00,
        "kvahconsumptionindustrial": merged_dict.get("kvahconsumptionindustrial") or 0.00,
        "kvahconsumptioncommercial": merged_dict.get("assessedconsumptionkwh") or 0.00,
        "kwhconsumptionresidential": merged_dict.get("assessedconsumptionkvah") or 0.00,
        "kwtotal": merged_dict.get("kwtotal") or 0.00,
        "kvatotal": merged_dict.get("kvatotal") or 0.00,
        "billeddemand": merged_dict.get("billeddemand") or 0.00,
        "billedpf": merged_dict.get("billedpf") or 0.00,
        "todconsumptionzone1": merged_dict.get("todconsumptionzone1") or 0.00,
        "todconsumptionzone2": merged_dict.get("todconsumptionzone2") or 0.00,
        "todconsumptionzone3": merged_dict.get("todconsumptionzone3") or 0.00,
        "todconsumptionzone4": merged_dict.get("todconsumptionzone4") or 0.00,
        "todconsumptionzone5": merged_dict.get("todconsumptionzone5") or 0.00,
        "todconsumptionzone6": merged_dict.get("todconsumptionzone6") or 0.00,
        "todconsumptionzone7": merged_dict.get("todconsumptionzone7") or 0.00,
        "todconsumptionzone8": merged_dict.get("todconsumptionzone8") or 0.00,        
        "loadfactor": merged_dict.get("loadfactor") or 0.00,
        "toddemandzone1": merged_dict.get("toddemandzone1") or 0.00,
        "toddemandzone2": merged_dict.get("toddemandzone2") or 0.00,
        "toddemandzone3": merged_dict.get("toddemandzone3") or 0.00,
        "toddemandzone4": merged_dict.get("toddemandzone4") or 0.00,
        "totalconsumedunits": total_consumed_units,
        "pfbaseline": 1,
        "loadfactorbaseline": 0
    }

    staticinformation = {
        "billdate":int(datetime.strptime(merged_dict.get("billdate"), '%Y-%m-%d').timestamp()),
        "billdatestart":int(datetime.strptime(merged_dict.get("billmonth"), '%b-%Y').replace(day=1).timestamp()),
        "billdateend": int(datetime.strptime(merged_dict.get("billmonth"), '%b-%Y').replace(day=calendar.monthrange(datetime.strptime(merged_dict.get("billmonth"), '%b-%Y').year, datetime.strptime(merged_dict.get("billmonth"), '%b-%Y').month)[1]).timestamp()),
        "sactionedload": merged_dict.get("sactionedload") or 0.00,
        "connectedload": merged_dict.get("connectedload") or 0.00,
        "contractdemand": merged_dict.get("contractdemand") or 0.00,
        "feedervoltage": merged_dict.get("feedervoltage") or 0.00,
        "percent_of_contractdemand": merged_dict.get("percent_of_contractdemand") or 0.00,
        "industrialconsumptionrate": merged_dict.get("industrialconsumptionrate") or 0.00,
        "residentialconsumptionrate": merged_dict.get("residentialconsumptionrate") or 0.00,
        "commercialconsumptionrate": merged_dict.get("commercialconsumptionrate") or 0.00,
        "wheelingchargesrate": merged_dict.get("wheelingchargesrate") or 0.00,
        "fac": merged_dict.get("facrate") or 0.00,
        "todratezone1": merged_dict.get("todratezone1") or 0.00,
        "todratezone2": merged_dict.get("todratezone2") or 0.00,
        "todratezone3": merged_dict.get("todratezone3") or 0.00,
        "todratezone4": merged_dict.get("todratezone4") or 0.00,
        "totalconsumeunitrate": (merged_dict.get("industrialconsumptionrate") or 0.00) + 
                                (merged_dict.get("residentialconsumptionrate") or 0.00) +
                                (merged_dict.get("commercialconsumptionrate") or 0.00),
        "demandrate": (
            round(commercials.get('demandcharges', 0.00) / consumptioninformation.get('billeddemand', 0.00),2)
            if consumptioninformation.get('billeddemand', 0.00) != 0.00
            else 0.00
        )
    }
    
    return {
            'staticinformation': staticinformation, 
            'consumptioninformation': consumptioninformation, 
            'commercials': commercials
        }
