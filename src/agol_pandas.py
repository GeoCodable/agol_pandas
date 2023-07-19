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
    try:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
            return (f.name, True)
    except Exception as e:
        return (str(e), False)
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
    try:
        cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]

        for col in cols:
            try:
                if df[col].dtype == 'datetime64[ns]':
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize('UTC')
            except:
                pass
            else:
                try:
                    df[col] = pd.to_datetime(df[col])
                    df[col] = df[col].dt.tz_convert('UTC')
                except:
                    pass
        return(df, True)    
    except Exception as e:
        print(e)
        return (df, False)
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
    try:
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
        return (service_name, True)
    except Exception as e:
        print(e)
        return (service_name, False)        
#--------------------------------------------------------------------------------
def df_to_pandas_chunks(df, chunk_size=100000, keys=[]):
    """
    Generator that sorts and then chunks a PySpark 
    or pandas DataFrame into DataFrames of the given
    chunk size.

    Parameters
    ----------
    df : pd.DataFrame or pyspark.sql.DataFrame
        The dataframe to sort and chunk.
    chunk_size: int
        The max size of each chunk
    keys: str or list
        Column name or list of column names to sort 
        a dataframe on before chunking.
        Default, None - Sorting will not be applied
    Returns
    -------
    generator : A generator that yields chunks of pandas DataFrames.
    """
    print(f'Generating chunks from dataframe')

    # Check if the dataframe is empty
    if isinstance(df, pd.DataFrame):
        total_rows = len(df)
        if total_rows == 0:
            raise ValueError("The dataframe is empty.")
 
    try:
        # if a key was supplied, sort the dataframe
        if bool(keys):
            if not isinstance(keys, list):
                keys = [keys]
                
        # sort and yield chunked pandas dataframes from pyspark
        if not isinstance(df, pd.DataFrame):
            df = df.orderBy(keys)
            total_rows = df.count()
            if total_rows == 0:
                raise ValueError("The dataframe is empty.")
            for i in range(0, df.count(), chunk_size):
                chunk = df.toPandas()[i:i + chunk_size]
                yield chunk
        else:
            # sort and yield chunked pandas dataframes 
            df = df.sort_values(by=keys)
            total_rows = len(df)
            if total_rows == 0:
                raise ValueError("The dataframe is empty.")
            for i in range(0, len(df), chunksize):
                chunk = df[i:i + chunksize]
                yield chunk
        chunk_cnt = int(total_rows/chunk_size) + sum(1 for r in [total_rows % chunk_size] if r>0)
        print(f'Generated {chunk_cnt:,} chunks of {chunk_size:,} (or less) rows from {total_rows:,} total records')
    except Exception as e:
        return str(e)   
#---------------------------------------------------------------------------------- 
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
    try:
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
        return (query_results.sdf, True)
    except Exception as e:
        return (str(e), False) 
#-------------------------------------------------------------------------------- 
def set_unique_key_constraint(gis, table_id, key_field_name):
    """
    Function adds a unique key constraint to the specified hosted table or layer.
    
    Parameters:
        table_id: str: 
            The ID of the table.
        key_field_name str: 
            The name of the field to add the unique constraint to.
    
    Returns:
        bool: 
            True if the constraint was created successfully, False otherwise.
    
    **Example:**
    
    >>> set_unique_key_constraint(gis, 'my_table_id', 'my_field_name')
    True
    
    **Notes:**
    
    * The function checks if the field already has a unique index before creating a new one.
    * The function waits for the index to be created before returning.
    """
    try:
        item = gis.content.get(table_id) 

        # determine if the item has layers/tables
        if bool(item.layers):
            tgt_table = item.layers[0] 
        if bool(item.tables):
            tgt_table = item.tables[0]
        
        def fld_has_unique_idx(key_field_name):
            idx_fld_names = [f.fields.lower() 
                            for f in tgt_table.properties.indexes 
                            if f.isUnique]
            return key_field_name.lower() in idx_fld_names
        
        if not fld_has_unique_idx(key_field_name):
            idxName = f'UX_{item.title.upper()}_{tgt_table._lazy_properties.name.upper()}_{key_field_name}_ASC'
            print(f'Adding index to {tgt_table._lazy_properties.name} on field "{key_field_name}" named as "{idxName}"')
            new_idx = {}
            new_idx['name'] = idxName
            new_idx['fields'] = key_field_name
            new_idx['isUnique'] = True
            new_idx['description'] = "Field properties"
            tgt_table.manager.add_to_definition({"indexes":[new_idx]})

            for x in range(12): # attempt every 5 secs for 1 min
                time.sleep(5)
                status = fld_has_unique_idx(key_field_name)
                if status:
                    print('\t-Index created successfully!')
                    break
            return (status, True)
        else:
            return (True, True)
    except Exception as e:
        return (str(e), False)
#--------------------------------------------------------------------------------
def df_to_agol_hosted_table(gis, df, item_id, mode='append', 
                            upsert_column=None, chunk_size=100000,
                            item_properties={}):
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
        if isinstance(df, pd.DataFrame):
            total_rows = len(df)
            if total_rows == 0:
                raise ValueError("The dataframe is empty.")
        else:
            total_rows = df.count()
            if total_rows == 0:
                raise ValueError("The dataframe is empty.")
                    
        # get the target item table
        # item = gis.content.search(item_id)[0]
        item = gis.content.get(item_id) 

        if item:
            # determine if the item has layers/tables
            if bool(item.layers):
                tgt_table = item.layers[0]
            if bool(item.tables):
                tgt_table = item.tables[0]
        else:
            print(f'Item with ID {item_id} not found')
            
        # set the append params
        upsert=False
        skip_inserts=False
        skip_updates=False
        upsert_matching_field=None

        update_modes = ['upsert', 'update', 'insert']
        if mode == 'overwrite':
            tgt_table.manager.truncate()
    
        elif mode in update_modes:
            if not upsert_column:
                raise ValueError("""Upsert, update, and insert, require a column with unique keys must be identified.\n
                                 See: https://doc.arcgis.com/en/arcgis-online/manage-data/add-unique-constraint.htm""")
            if mode =='update':
                skip_inserts=True
            if mode =='insert':
                skip_updates=True            
            upsert=True
            upsert_matching_field=upsert_column
            
            #---------------------------
            idx_test = set_unique_key_constraint(gis, item_id, upsert_column)
            if not idx_test:
                raise ValueError(f"Unique field constraint required for {update_modes}!")
            #---------------------------      
        
        # Split the dataframe into chunks
        if total_rows > chunk_size:
            if upsert_column: 
                key_flds = [upsert_column]
            chunks = df_to_pandas_chunks(df, chunk_size=chunk_size, keys=key_flds)
        else:
            chunks = [df]
        if not bool(chunks):
            raise ValueError("The dataframe could not be chunked, see chunk_size")
        rec_loaded = 0
        # iterate the chunks and apply the data from the dataframe
        for idx, chunk in enumerate(chunks):
                       
            # create a temp csv file path
            tmp_csv, pStatus = get_temp_file()
            if not pStatus:
                raise Exception(tmp_csv)
                
            chunk.to_csv(tmp_csv)
            # set the item properties dataframe
            if not bool(item_properties):
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
            rec_loaded += len(chunk)
            print(f'Loaded {rec_loaded:,} of {total_rows:,} rows', end='\r')
            tmp_table.delete()
            r_dict = {'chunk_id': (idx+1), 'chunk_size': len(chunk),
                      'mode' : mode, 'result': result}
            results.append(r_dict)
        return (results, True)
    except Exception as e:
        return (str(e), False) 
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
def create_table(gis, name, df, key_field_name, item_properties={}):
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

        # Check if the dataframe is empty
        if isinstance(df, pd.DataFrame):
            total_rows = len(df)
            if total_rows == 0:
                raise ValueError("The dataframe is empty.")
        else:
            total_rows = df.count()
            df = df.toPandas()
            if total_rows == 0:
                raise ValueError("The dataframe is empty.")
        
        tmp_csv = None
        tmp_table = None
        # create a temp csv file path
        tmp_csv, pStatus = get_temp_file()
        if not pStatus:
            raise Exception(tmp_csv)        
        
        # export he dataframe to csv
        df.to_csv(tmp_csv)
        # set the item properties dataframe
        item_properties["title"] = name
        # add/upload the csv to the user's content 
        tmp_table = gis.content.add(data=tmp_csv, 
                                    item_properties=item_properties,
                                    owner=gis.users.me.username)
        # publish the csv as a hoasted table
        pub_table = tmp_table.publish(None)
        # remove the temp csv file
        os.remove(tmp_csv)
        #---------------------------
        idx_test = set_unique_key_constraint(gis, pub_table.id, key_field_name)
        if not idx_test:
            raise ValueError("Could not create unique field constraint for appends!")
        #---------------------------        
        return (pub_table, True)
    except Exception as e:
        return (str(e), False) 
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
def create_hosted_table_from_dataframe(gis: GIS,  df: pd.DataFrame, name: str = None, table_id=None, 
                                      chunk_size: int = 200000,key_field_name: str = None,
                                      item_properties: dict ={}):
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
        if isinstance(df, pd.DataFrame):
            total_rows = len(df)
            if total_rows == 0:
                raise ValueError("The dataframe is empty.")
        else:
            total_rows = df.count()
            if total_rows == 0:
                raise ValueError("The dataframe is empty.")
    
        if not bool(name) or bool(table_id):
            raise ValueError("An item ID or name is required.")
        if table_id:
            pub_table = gis.content.get(table_id)
        elif name:
            # format the service name
            tbl_name, pStatus = normalize_service_name(name)
            if not pStatus: 
                    print('Failed to normalize service name')       
            pub_table = None
    
            items = gis.content.search(query=f"title:{tbl_name} AND type:Feature Service AND owner:{gis.users.me.username}")
            items = [i for i in items if i.title==tbl_name]
            if len(items) > 0 :
                table_id = items[0].id
                pub_table = items[0]
    
        # Split the dataframe into chunks
        if total_rows > chunk_size:
            chunks = df_to_pandas_chunks(df, chunk_size=chunk_size, keys=[key_field_name])
            if not key_field_name:
                mode = 'append'
            else:
                mode = 'upsert'
        else:
            chunks = [df]
        if not bool(chunks):
            raise ValueError("The dataframe could not be chunked, see chunk_size")
    
        chnk_results = []

        rec_loaded = 0
        # create a new table or update an existing using the first chunk, append for subsequent chunks 
        for idx, chunk in enumerate(chunks):
        
            # Sort the IDs in ascending order
            sorted_uids = chunk[key_field_name].sort_values()
        
            cr = {'chunk_id': (idx+1), 
                  'chunk_size': len(chunk),
                  'row_start': chunk.index[0] + 1,
                  'row_end': chunk.index[-1] + 1,
                  'start_id': sorted_uids.iloc[0],
                  'end_id': sorted_uids.iloc[-1],                  
                  'Success' : False}        

                    
            if idx == 0 and not bool(table_id):
                cr['mode'] ='create'
                pub_table, pStatus = create_table(gis=gis, 
                                                  name=tbl_name, 
                                                  df=chunk, 
                                                  key_field_name=key_field_name,
                                                  item_properties=item_properties )
                if not pStatus:
                    raise ValueError("Table could not be created")
                else:
                    table_id = pub_table.id
                    print(f'Item created with name:({tbl_name}) and Item ID: ({table_id})')
                    rec_loaded += len(chunk)
                    print(f'Loaded {rec_loaded:,} of {total_rows:,} rows', end='\r')
            else: 
                results, pStatus = df_to_agol_hosted_table( gis=gis, 
                                                            df=chunk, 
                                                            item_id=table_id, 
                                                            mode=mode,
                                                            chunk_size=chunk_size,
                                                            upsert_column=key_field_name,
                                                            item_properties=item_properties
                                                           ) 
                rec_loaded += len(chunk)
                print(f'Loaded {rec_loaded:,} of {total_rows:,} rows', end='\r')
                cr['Messages'] = results
                cr['mode'] = mode
            cr['Success'] = pStatus
            cr['item_id'] = table_id
            chnk_results.append(cr)
        return (chnk_results, True)             
    except Exception as e:
        return (str(e), False) 
        return(e)
#-------------------------------------------------------------------------------            
