
import json  
import boto3                #   Amazon SDK
import pandas as pd         #   Formatting of excel file and performing operation on excel
import io                   #   Handling Input/Output
from io import BytesIO 
import botocore             #   ErrorHandling
import xlsxwriter           #   Writing to Excel
import awswrangler          #   Importing Openpyxl and performing complex read operations on it 

key = 'Account Details for Volume Conversion.xlsx'                 # ^^^^INPUT FOLDER NAME/INPUT DOCUMENT NAME
bucket = 'buckettest'                                         # ^^^^BUCKET NAME
                    
s3=boto3.client('s3')   
file_object =s3.get_object(Bucket=bucket, Key=key)
file_content = file_object['Body'].read()
b_file_content = io.BytesIO(file_content)                                
df= pd.read_excel(b_file_content)                                                     # df = dataframe for pandas
df_sheet_index = pd.read_excel(b_file_content, sheet_name=0)                          # first sheet from excel is read
h_column_list_of_excel_file = df_sheet_index.columns.ravel().tolist()
b_file_content.close()    

acc_id=[]
acc_name=[]
account_id = []
name_missing_list = []
Comments = []
Reason_for_error = []
account_ID = []
Flag_for_name = False
Flag_for_ec2_permission_role_error = True 
acc_id_causing_error = []
acc_name_causing_error =[]
serial_number_for_comments_sheet = []
serial_number_for_comments = 0
Flag_for_id = False  
id_missing_list = [] 
accId=[] 
accName = [] 
accid_from_excel=df_sheet_index[h_column_list_of_excel_file[1]].tolist()
accName_from_excel=df_sheet_index[h_column_list_of_excel_file[2]].tolist() 
print(accid_from_excel) 
for i in range(len(accid_from_excel)):
    if pd.isnull(accid_from_excel[i]) == False :    
        accId.append(int(accid_from_excel[i])) 
        accName.append(accName_from_excel[i])
    else: 
        id_missing_list.append(i+1)
        Flag_for_id = True 
        Reason_for_error.append("Account Id Missing") 
        Comments.append("Account Id Missing at {}".format(i+1))
        acc_name_causing_error.append(accName_from_excel[i]) 
        acc_id_causing_error.append("")
        serial_number_for_comments = serial_number_for_comments + 1 
        serial_number_for_comments_sheet.append(serial_number_for_comments) 
    
print(accId)

for each in range(len(accName)):                 # for finding the missing entries in acc name and account id in the input excel
    if pd.isnull(accName[each])== False :   
        account_ID.append(accId[each])
        acc_name.append(accName[each]) 
    else:
        name_missing_list.append(i+1)
        Flag_for_name = True 
        Reason_for_error.append("Account Name Missing") 
        Comments.append("Account Name Missing at {}".format(each+1))
        acc_name_causing_error.append("")  
        acc_id_causing_error.append(accId[each])  
        serial_number_for_comments = serial_number_for_comments + 1 
        serial_number_for_comments_sheet.append(serial_number_for_comments) 
print(account_ID)         
for each in account_ID:
    account_id.append(str(each))
print(account_id) 

client = boto3.client('sts')
master_acc_id = client.get_caller_identity()['Account']
print(master_acc_id) 

for each in account_id:
    if len(each)==12:
        acc_id.append(each)
    else :
        N=12-len(each)
        each = each.rjust(N + len(each), '0')
        acc_id.append(each)  
  
rolearn = []  
for each in range(len(acc_id)):
    if acc_id[each] != master_acc_id:
        rolearn.append("arn:aws:iam::{}:role/Cross_Account_Role".format(acc_id[each]))   # ^^^ROLE NAME
dict_for_name = dict(zip(acc_id,acc_name))        
print(rolearn)
Flag_for_role_error = False
Flag_for_ec2_permission_role_error = False

#--------------------Conversion work Report---------------------------------------------------------------------------

def volume_conversion(): 
    serial_number_for_comments_new = serial_number_for_comments
    serial_number = 0
    serial_number_stored_in_xlsx = [] 
    acc_id_stored_in_xlsx = []
    acc_name_stored_in_xlsx = []  
    VolumeId_stored_in_xlsx = []  
    volume_with_status = []
    volume_after_changing = []
    Region_stored_in_xlsx = []
    snapshot_id_stored_in_xlsx = []
    size_stored_in_xlsx = []
    status_stored_in_xlsx = []
    for each in range(len(rolearn)): 
        try:                 
            sts_connection = boto3.client('sts')                                #temporary credentials 
            acct_b = sts_connection.assume_role(
            RoleArn=rolearn[each],     
            RoleSessionName="Cross_Account_Role"                               # ^^^^ROLE NAME
            )   
            
            ACCESS_KEY = acct_b['Credentials']['AccessKeyId']
            SECRET_KEY = acct_b['Credentials']['SecretAccessKey']    
            SESSION_TOKEN = acct_b['Credentials']['SessionToken']
    
            client = boto3.client('ec2',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            aws_session_token=SESSION_TOKEN,
                )
                
            ACC_ID = rolearn[each].split(":")[4]
            ec2_regions = [region['RegionName'] for region in client.describe_regions()['Regions']]
            
            try:
                
                for region in ec2_regions:  
                    client = boto3.client('ec2',region,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    aws_session_token=SESSION_TOKEN,
                        )
                    ec2 = boto3.resource('ec2',region,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    aws_session_token=SESSION_TOKEN,
                    )
                    # client = boto3.client('ec2',region) 
                    # ec2 = boto3.resource('ec2',region)
                    volumes = ec2.volumes.filter(Filters=[{'Name': 'volume-type', 'Values': ['gp2']}]) #only gp2 volumes are filtered
                    volume_ids = [v.id for v in volumes]
                       
                    # print(volume_ids,region) 
                   
                                                           #snapshot taken for gp2 volume
                        
                    for i in volume_ids:                                         #volume modification work
                        response = ec2.create_snapshot(
                        VolumeId=i
                        )
                        v = str(response)
                        z=v.split("=")
                        snap=z[1].split(")")
                        print(snap[0]) 
                        client.modify_volume(
                            VolumeId=i,
                            VolumeType='gp3'
                            )
                        volume = ec2.Volume(i)
                        serial_number = serial_number + 1
                        serial_number_stored_in_xlsx.append(serial_number)
                        acc_id_stored_in_xlsx.append(ACC_ID)
                        for ac_id,name in dict_for_name.items(): 
                            if ac_id == ACC_ID: 
                                acc_name_stored_in_xlsx.append(name)
                        Region_stored_in_xlsx.append(region)
                        VolumeId_stored_in_xlsx.append(i)
                        volume_with_status.append(volume.volume_type) 
                        volume_after_changing.append('gp3')
                        x=ec2.Snapshot
                        snapshot_id_stored_in_xlsx.append(snap[0])  
                        size_stored_in_xlsx.append(volume.size)
                        status_stored_in_xlsx.append(volume.state)
                        
            except botocore.exceptions.ClientError as error:
                Flag_for_ec2_permission_role_error = True
                Comments.append(error)
                serial_number_for_comments_new = serial_number_for_comments_new + 1
                serial_number_for_comments_sheet.append(serial_number_for_comments_new)  
                Reason_for_error.append("EC2/EBS Permission Related")
                ACC_ID = rolearn[each].split(":")[4] 
                acc_id_causing_error.append(ACC_ID)
                for ac_id,name in dict_for_name.items(): 
                    if ac_id == ACC_ID: 
                        acc_name_causing_error.append(name) 
                
        except botocore.exceptions.ClientError as error:
            Flag_for_role_error = True
            print(error) 
            Comments.append(error)
            Reason_for_error.append("Assume Role Issue")
            serial_number_for_comments_new = serial_number_for_comments_new + 1
            serial_number_for_comments_sheet.append(serial_number_for_comments_new)
            ACC_ID = rolearn[each].split(":")[4]
            acc_id_causing_error.append(ACC_ID)
            for ac_id,name in dict_for_name.items(): 
                if ac_id == ACC_ID: 
                    acc_name_causing_error.append(name)  
            
                                                 # for master account
    for i in range(len(acc_id)):
        if acc_id[i]==master_acc_id:
            client = boto3.client('ec2')
            try:
                ec2_regions = [region['RegionName'] for region in client.describe_regions()['Regions']]
                for region in ec2_regions:  
                    client = boto3.client('ec2',region)
                    ec2 = boto3.resource('ec2',region)
                    volumes = ec2.volumes.filter(Filters=[{'Name': 'volume-type', 'Values': ['gp2']}])
                    volume_ids = [v.id for v in volumes]              #list of volumes with gp2
                    
                        
                    for vi in volume_ids:
                        response = ec2.create_snapshot(                   #snapshot creation
                        VolumeId=vi
                        )
                        print(response) 
                        client.modify_volume(                             #volume modification
                            VolumeId=vi,
                            VolumeType='gp3'
                            ) 
                            
                        v = str(response)
                        z=v.split("=")
                        snap=z[1].split(")")
                        print(snap[0]) 
                        serial_number = serial_number + 1
                        serial_number_stored_in_xlsx.append(serial_number)
                        acc_id_stored_in_xlsx.append(acc_id[i])
                        acc_name_stored_in_xlsx.append(acc_name[i])
                        Region_stored_in_xlsx.append(region)
                        VolumeId_stored_in_xlsx.append(vi)
                        volume = ec2.Volume(vi)
                        volume_with_status.append(volume.volume_type) 
                        volume_after_changing.append('gp3')
                        x=ec2.Snapshot 
                        snapshot_id_stored_in_xlsx.append(snap[0]) 
                        size_stored_in_xlsx.append(volume.size)
                        status_stored_in_xlsx.append(volume.state)
            except botocore.exceptions.ClientError as error:
                Flag_for_ec2_permission_role_error = True
                Comments.append(error)
                serial_number_for_comments_new = serial_number_for_comments_new + 1
                serial_number_for_comments_sheet.append(serial_number_for_comments_new)  
                Reason_for_error.append("EC2/EBS Related")
                acc_id_causing_error.append(acc_id[i])
                acc_name_causing_error.append(acc_name[i]) 
    
    print(Comments)
    print(len(serial_number_stored_in_xlsx))
    print(len(acc_id_stored_in_xlsx))
    print(len(acc_name_stored_in_xlsx))
    print(len(VolumeId_stored_in_xlsx))
    print(len(Region_stored_in_xlsx))
    print(len(volume_with_status))
    print(len(volume_after_changing))
    print(len(size_stored_in_xlsx))
    
     #excel storage work
    data={'S No ':serial_number_stored_in_xlsx, 'Account Id':acc_id_stored_in_xlsx, 'Account Name':acc_name_stored_in_xlsx,'Region':Region_stored_in_xlsx,'Volume Id': VolumeId_stored_in_xlsx,'Volume before changing': volume_with_status, 'Volume after changing ': volume_after_changing,'Snapshot ID':snapshot_id_stored_in_xlsx, 'Size': size_stored_in_xlsx, 'State': status_stored_in_xlsx}
    data_frame=pd.DataFrame(data)
    
    data_for_error={'S.No':serial_number_for_comments_sheet, 'Account Id':acc_id_causing_error,'Account Name':acc_name_causing_error,'Possible Cause ':Reason_for_error, 'Comments':Comments}
    data_frame_error=pd.DataFrame(data_for_error)
    
    io_buffer = io.BytesIO()   
    s3 = boto3.resource('s3')  
    writer = pd.ExcelWriter(io_buffer, engine='xlsxwriter')
    sheets_in_writer=['Status','Comments']
    data_frame_for_writer=[data_frame, data_frame_error]
    for i,j in zip(data_frame_for_writer,sheets_in_writer):
        i.to_excel(writer,j,index=False)    
    workbook=writer.book
    header_format = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'9ACD32','border': 1})
    max_col=4   
    header_format_comments = workbook.add_format({'bold': True,'text_wrap': True,'size':12, 'font_color':'black','valign': 'center','fg_color':'F2FBA1','border': 1}) 
    
    
    worksheet=writer.sheets["Status"]   
    
    for col_num, value in enumerate(data_frame.columns.values): 
        worksheet.write(0, col_num, value, header_format) 
        worksheet.set_column(1, 9, 20)
        worksheet.set_column(4,4,40) 
        
        
        
    worksheet=writer.sheets["Comments"]  
    
    for col_num, value in enumerate(data_frame_error.columns.values): 
        worksheet.write(0, col_num, value, header_format_comments)  
        worksheet.set_column(0,2,15)  
        worksheet.set_column(3,3,25)  
        worksheet.set_column(4,4,45)   
        
    filepath = 'Volume Conversion Report.xlsx'                               #document name
    writer.save()     
    data = io_buffer.getvalue() 
    s3.Bucket('buckettest').put_object(Key=filepath, Body=data)         #specify the bucket name
    io_buffer.close()   
    volume_conversion.has_been_called = True 
    #  
    # region = 'ap-northeast-2'   
    # client = boto3.client('ec2',region)
    # ec2 = boto3.resource('ec2',region)
    # volumes = ec2.volumes.filter(Filters=[{'Name': 'volume-type', 'Values': ['gp2']}])
    # volume_ids = [v.id for v in volumes]
    
    # print(volume_ids,region) 
   
    # for i in volume_ids:
    #     response = ec2.create_snapshot(
    #     VolumeId=i
    #     )
    # for i in volume_ids:
    #     client.modify_volume(
    #         VolumeId=i,
    #         VolumeType='gp3'
            # ) 
    
def lambda_handler(event, context):
    
    volume_conversion.has_been_called = False
    
    volume_conversion()    #function called to perform the storage conversion
    
    if volume_conversion.has_been_called:
        return "Volume Converted"
    else:
        return "No operation done" 
