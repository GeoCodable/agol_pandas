import os, uuid, re
import pandas as pd
import tempfile
from arcgis.gis import GIS


def get_temp_file(suffix: str = ".csv"):
    """
    Returns a path to a temporary file in the default temp directory.

    Parameters
    ----------
    suffix : str 
    Returns:
    A path to a temporary file.
    """
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
        return f.name
#--------------------------------------------------------------------------------
def convert_dts_utc(df: pd.DataFrame):
    """
    Converts all datetime columns in a Pandas dataframe to UTC timezone.

    Parameters
    ----------
    df : pd.DataFrame
        The Pandas dataframe to convert.

    Returns
    -------
    df : pd.DataFrame
        The Pandas dataframe with all datetime columns converted to 
        UTC timezone.
    """   
    cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]

    for col in cols:

        if df[col].dtype == 'datetime64[ns]':

            df[col] = pd.to_datetime(df[col]).dt.tz_localize('UTC')

        else:

            df[col] = pd.to_datetime(df[col])

            df[col] = df[col].dt.tz_convert('UTC')

    return(df)    
#--------------------------------------------------------------------------------
def normalize_service_name(service_name: str):
    """
    Normalizes a service name to follow the ArcGIS naming 
    convention rules.  Service names must be a validated,
    which means it must only contain letters, numbers,
    and/or underscores while being no longer than 128 
    characters.

    Parameters
    ----------
    service_name : str
        The name of the service to normalize.

    Returns
    -------
    str
        The normalized service name.
    """
    # Remove all leading and trailing whitespace.
    service_name = service_name.strip()
    # ensure the name does not start with a number
    if service_name[0].isdigit():
        service_name = f'_{service_name}'
    # Preplace all characters that are not letters, numbers, or underscores.
    service_name = re.sub(r"[^\w]", "_", service_name)
    # Replace all consecutive underscore characters wit a single underscore.
    service_name = re.sub('_+', '_', service_name)
    # Convert the name to lowercase.
    service_name = service_name.lower()
    # Truncate the name to 128 characters.
    if len(service_name) > 128:
        service_name = service_name[:128]
    return service_name
#--------------------------------------------------------------------------------
def agol_hosted_item_to_sdf(gis: GIS, item_id: str):
    """
    Reads all data from a hosted layer or tableon ArcGIS Online 
    into a Pandas dataframe.

    Parameters
    ----------
    gis : GIS
        The ArcGIS object to use for connecting to ArcGIS Online.
    item_id : str
        The ID of the hosted layer or table on ArcGIS Online.

    Returns
    -------
    sdf : pd.DataFrame
        A Pandas dataframe containing the data from the hosted layer.
    """
    # Get the layer object from ArcGIS Online.
    item = gis.content.get(item_id)

    # determine if the item has layers/tables
    if bool(item.layers):
        table = item.layers[0]    
    if bool(item.tables):
        table = item.tables[0]

    # Get the query results from the layer.
    query_results = table.query(return_all_records=True)

    # Return the query results as a Pandas dataframe.
    return query_results.sdf
#-------------------------------------------------------------------------------- 
def df_to_agol_hosted_table(gis, df, item_id, mode='append', 
                            upsert_column=None,  chunk_size: int = 5000):
    """
    Function will "append", "overwrite", "upsert", 
    "update", or "insert" data from a pandas dataframe
    into an existing hosted ArcGIS Online table.
    
    Parameters
    ----------
    gis : ArcGIS python api portal GIS object, required
        ArcGIS python api portal GIS object
    df : Pandas dataframe or object, required 
    item_id : str, required
        AGOL resource/item ID for a table   
    mode : str, optional
        Data append method, options include "append", "overwrite",
        "upsert", "update", and "insert"  
    upsert_column : str, *optional
        Name of the unique key column required to use "upsert", 
        "update", or "insert" modes      
    chunk_size : int, optional
        The number of rows to include in each chunk. If not specified, 
        a default chunk size will be used.               
    Returns
    -------
    result : list
        List containing a dictionary detailing the results of each 
        attempt to push data into the target table to include the
        chunk id, chunk size, mode, and the Boolean result where
        True = success.
        Example:
            [
              {
                'chunk_id': 1, 
                'chunk_size': 500,
                'mode' : 'append', 
                'result': True
              }
            ]                              
    """        
    try:
        results = []
        tmp_csv = None
        tmp_table = None
    
        # check the supplied mode
        modes = ["append", "overwrite", "upsert", "update", "insert"]
        if mode not in modes:
            raise ValueError(f'Unidentified mode supplied: "{mode}"')
    
        # Check if the dataframe is empty
        if len(df) == 0:
            raise ValueError("The dataframe is empty.")
    
        # attempt to convert datetime stamps to UTC TZ for AGOL
        try:
            df = convert_dts_utc(df)
        except:
            pass
    
        # get the target item table
        # item = gis.content.search(item_id)[0]
        item = gis.content.get(item_id) 

        if item:
            # determine if the item has layers/tables
            if bool(item.layers):
                tgt_table = item.layers[0]
                print(f'layers : {item.layers[0]}')
            if bool(item.tables):
                tgt_table = item.tables[0]
                print(f'layers : {item.tables[0]}')
        else:
            print(f'Item with ID {item_id} not found')
            
        # set the append params
        upsert=False
        skip_inserts=False
        skip_updates=False
        upsert_matching_field=None
    
        if mode == 'overwrite':
            tgt_table.manager.truncate()
    
        elif mode in ['upsert', 'update', 'insert']:
            if not upsert_column:
                raise ValueError("""Upsert, update, and insert, require a column with unique keys must be identified.\n
                                 See: https://doc.arcgis.com/en/arcgis-online/manage-data/add-unique-constraint.htm""")
            if mode =='update':
                skip_inserts=True
            if mode =='insert':
                skip_updates=True            
            upsert=True
            upsert_matching_field=upsert_column
    
        # Split the dataframe into chunks
        if len(df) > chunk_size:
            chunks = [df[i:i+chunk_size] for i in range(0,df.shape[0],chunk_size)]
        else:
            chunks = [df]
        if not bool(chunks):
            raise ValueError("The dataframe could not be chunked, see chunk_size")
    
        # iterate the chunks and apply the data from the dataframe
        for idx, chunk in enumerate(chunks):
            # create a temp csv file path
            tmp_csv = get_temp_file()
            chunk.to_csv(tmp_csv)
            # set the item properties dataframe
            item_properties = {"title" : tmp_csv}
            # add/upload the csv to the user's content 
            tmp_table = gis.content.add(data=tmp_csv , 
                                         item_properties=item_properties)
            # get info about the file including fields types and sample records
            src_info = gis.content.analyze(item=tmp_table.id, 
                                           file_type='csv', 
                                           location_type='none')
    
            result = tgt_table.append(  item_id=tmp_table.id,
                                        upload_format="csv",
                                        source_info=src_info['publishParameters'],
                                        upsert=upsert,
                                        skip_updates=skip_updates,
                                        use_globalids=False,
                                        update_geometry=False,
                                        append_fields= df.columns.to_list(),
                                        rollback=True,
                                        skip_inserts=skip_inserts,
                                        upsert_matching_field=upsert_matching_field)
            tmp_table.delete()
            results.append({'chunk_id': (idx+1), 
                            'chunk_size': len(chunk),
                            'mode' : mode, 
                            'result': result})
        return results
    except Exception as e:
        print(e)
    finally:
        try:
            if tmp_csv and os.path.exists(tmp_csv):
                os.remove(tmp_csv)
        except:
            pass                
        try: 
            if bool(tmp_table):
                tmp_table.delete()
        except:
            pass
#-------------------------------------------------------------------------------
def create_table(gis: GIS, name: str, df: pd.DataFrame, item_properties=None):
    """Internal function to upload a new
    csv and create a new hoasted table
    Parameters
    ----------   
    gis : arcgis.gis.GIS
        The GIS object to use for creating the feature service.       
    name : str
        The name to use for the new feature service.
    df : pandas.DataFrame 
        The DataFrame containing the data to use for creating 
        the feature service.
    Returns
    -------
    pub_table : AGOL table item
        Published AGOL table item
    """
    try:
        tmp_csv = None
        tmp_table = None
        # create a temp csv file path
        tmp_csv = get_temp_file()
        # export he dataframe to csv
        df.to_csv(tmp_csv)
        # set the item properties dataframe
        item_properties = {"title" : name}
        # add/upload the csv to the user's content 
        tmp_table = gis.content.add(data=tmp_csv, 
                                    item_properties=item_properties,
                                    owner=gis.users.me.username,
                                    item_properties=item_properties)
        # publish the csv as a hoasted table
        pub_table = tmp_table.publish(None)
        # remove the temp csv file
        os.remove(tmp_csv)
        return pub_table
    except Exception as e:
        print(e)
        try: 
            if tmp_table:
                tmp_table.delete()
        except:
            pass
    finally:
        try:
            if tmp_csv and os.path.exists(tmp_csv):
                os.remove(tmp_csv)
        except:
            pass 
#-------------------------------------------------------------------------------      
def create_hosted_table_from_dataframe(gis: GIS, name: str, df: pd.DataFrame, 
                                      chunk_size: int = 5000):
    """
    Function creates a new feature service from data in a Pandas or 
    ArcGIS Spatial DataFrame.

    Parameters
    ----------
    gis : arcgis.gis.GIS
        The GIS object to use for creating the feature service.
    name : str
        The name to use for the new feature service.
    df : pandas.DataFrame 
        The DataFrame containing the data to use for creating the feature service.
    chunk_size : int, optional
        The number of rows to include in each chunk. If not specified, 
        a default chunk size will be used.

    Returns
    -------
    arcgis.gis.Item
        arcgis.gis table layer item/object
    """
    try:
        # Check if the dataframe is empty
        if len(df) == 0:
            raise ValueError("The dataframe is empty.")
    
        # format the service name
        tbl_name = normalize_service_name(name)
    
        # Check if the name is already in use
        name_avail = gis.content.is_service_name_available(tbl_name, "featureService")
        if not name_avail:
            qs = f'title:{name} AND owner:{gis.users.me.username} AND type:Feature Service'
            qr = gis.content.search(qs)[0]
            qr_link = f'{gis.url}/home/item.html?id={qr.itemid}'
            print(f'Error service name:({tbl_name}) already exists! SEE: {qr_link}')
            return qr
    
        # attempt to convert datetime stamps to UTC TZ for AGOL
        try:
            df = convert_dts_utc(df)
        except:
            pass
    
        # Split the dataframe into chunks
        if len(df) > chunk_size:
            chunks = [df[i:i+chunk_size] for i in range(0,df.shape[0],chunk_size)]
        else:
            chunks = [df]
        if not bool(chunks):
            raise ValueError("The dataframe could not be chunked, see chunk_size")
    
        # create a new table using the first chunk, append for subsequent chunks 
    
        items = gis.content.search(query=f"title:{tbl_name} AND type:Feature Service AND owner:{gis.users.me.username}")
        items = [i for i in items if i.title==tbl_name]
        if len(items) > 0 :
            table_id = items[0].id
            pub_table = items[0]
        else:
            table_id = None
            pub_table = None
        for idx, chunk in enumerate(chunks):
            if idx == 0 and not bool(table_id):
                pub_table = create_table(gis, name=tbl_name, df=chunk)
                if not pub_table:
                    raise ValueError("Table could not be published")
                table_id = pub_table.id
            else:
                df_to_agol_hosted_table(gis, 
                                        chunk, 
                                        table_id, 
                                        mode='append',
                                        chunk_size=chunk_size)
        return pub_table             
    except Exception as e:
        print(e)
        return(e)
