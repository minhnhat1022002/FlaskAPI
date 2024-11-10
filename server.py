from flask import Flask, flash, request, redirect, send_file, session, jsonify, after_this_request
from flask_cors import CORS
from flask_bcrypt import Bcrypt
from flask_session import Session
import json

from config import ApplicationConfig
from models import db, User

from werkzeug.utils import secure_filename

import os


# os.environ['PYSPARK_PYTHON'] = 'python3.9'
# os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3.9'
# from utilities import *
# import subprocess

app = Flask(__name__)


app.config.from_object(ApplicationConfig)
db.init_app(app)
with app.app_context():
    db.create_all()



bcrypt = Bcrypt(app)
CORS(app, supports_credentials=True)

basedir = os.path.abspath(os.path.dirname(__file__))

#-------------------------------------------------------------------------------------------------------------------------------------

@app.route("/register", methods=["POST"]) #sau khi register thanh cong se tu dong login
def register_user():
    global history
    #gets email and password input
    email = request.json["email"]
    password = request.json["password"]

    user_exists = User.query.filter_by(email=email).first() is not None

    if user_exists:
        return jsonify({"error": "User already exists"}), 409
    hashed_password = bcrypt.generate_password_hash(password)
    new_user = User(email=email, password=hashed_password)
    db.session.add(new_user)
    db.session.commit()
    
    session["user_id"] = new_user.id
    
    # tao thu muc de chua file upload truoc khi tai len hdfs
    os.makedirs(session['user_id']+'/upload/')
    
    # tao thu muc tren hdfs
    hadoop, conf, fs = configure_hadoop(spark)
    path = hadoop.fs.Path
    fs.mkdirs(path(session['user_id']+'/init/'))
    fs.mkdirs(path(session['user_id']+'/ksample/'))
    fs.mkdirs(path(session['user_id']+'/ncore/'))
    fs.mkdirs(path(session['user_id']+'/split/'))
    fs.mkdirs(path(session['user_id']+'/upload/')) # noi cac file upload duoc tai len
    
    '''
    os.makedirs(session['user_id']+'/init/')
    os.makedirs(session['user_id']+'/ksample/')
    os.makedirs(session['user_id']+'/ncore/')
    os.makedirs(session['user_id']+'/split/')
    '''
    history[new_user.id]={}
    
    return jsonify({
        "id": new_user.id,
        "email": new_user.email
    })
    
@app.route('/login',methods=['POST'])
def login():

    global history
    
        
    email = request.json["email"]
    password = request.json["password"]

    user = User.query.filter_by(email=email).first()

    if user is None:
        return jsonify({"error": "Unauthorized"}), 401
    #checking if the password is the same as hashed password
    if not bcrypt.check_password_hash(user.password, password):
        return jsonify({"error": "Unauthorized"}), 401
    
    
    if session.get('user_id') and session.get('user_id') != user.id:
        logout_user()
    session["user_id"] = user.id
    
    return jsonify({
        "id": user.id,
        "email": user.email
    }) 
    
    

@app.route("/logout", methods=["POST"])
def logout_user():
    global history
    if session['user_id'] in history:
        history.pop(session['user_id'])
    if 'user_id' in session:
        session.pop('user_id')
    
    return 'logout success', 200

@app.route("/@me")
def get_current_user(): 
    global history
        
    user_id = session.get("user_id")
    
    app.logger.info(user_id)
    
    if not user_id:
    
        # FOR DEVELOPMENT
        # session["user_id"] = 'dev' 
        
        return jsonify({"error": "Unauthorized"}), 401
        
    if session["user_id"] =='dev':
        
        if 'dev' not in history:
            history['dev']={}# tên file + thông tin stage, dataframe
            app.logger.info(history)
        
        app.logger.info(history['dev'])
        return jsonify({
            "id": 'devid',
            "email": 'devmode'
        }) 
        
    user = User.query.filter_by(id=user_id).first()
    if user.id not in history:
        history[user.id]={} # tên file + thông tin stage, dataframe
        app.logger.info(history)
    
    return jsonify({
        "id": user.id,
        "email": user.email
    }) 
@app.route('/api/checkfile') # scan all folders or files exist in a 
def checkfile():
    '''
    init = [x for x in next(os.walk(session.get('user_id')+'/init'))]
    ksample = [x for x in next(os.walk(session.get('user_id')+'/ksample'))]
    ncore = [x for x in next(os.walk(session.get('user_id')+'/ncore'))]
    split = [x for x in next(os.walk(session.get('user_id')+'/split'))]
    '''
    
    # upload = getfilelist(session.get('user_id')+'/upload')
    # init = getfilelist(session.get('user_id')+'/init')
    # ksample = getfilelist(session.get('user_id')+'/ksample')
    # ncore = getfilelist(session.get('user_id')+'/ncore')
    # split = getfilelist(session.get('user_id')+'/split')
    # tmp = [x for x in next(os.walk(session.get('user_id')+'/tmp'))]
    
    init = [x for x in next(os.walk(session.get('user_id')+'/init'))]
    
    return [upload, init, ksample, ncore, split]

# -----------------------------------------------------------------------------------------------------------------------------------
"""

@app.route('/api/loadfile/<stagename>/<stageno>/<filename>', methods=['GET'])
def loadfile(stagename,stageno,filename):
    if filename in history[session.get('user_id')]:
        info = history[session.get('user_id')][filename]
        return {'rootdf': info['info'],'stagesinfo': info['stagesinfo'] }
    
    history[session.get('user_id')][filename] = {}
    filename = secure_filename(filename)
    withheader = True
    if stageno == '3':
        stageno = '4'
        withheader = False
        
    try:
        df = spark.read.csv(session.get('user_id')+'/'+stagename+'/'+filename,header=withheader)
    except:
        return 'theres no file', 404
    addmsg = ''
    
    if stageno == '2' and 'item_id' in df.schema.names:
        stageno = '3'
        addmsg = ' with id remapped'
    
    
        
    info = {
        'rootdf': df,
        'info':{
            'name': 'This dataframe is loaded from stage '+str(int(stageno)+1)+' - '+stagename+addmsg,
            'currentstage': {'no':int(stageno),'name':stagename},
            'savedname':filename,
            'html': df.limit(10).toPandas().to_html()
        },
        'stagesdf':[None]*4,
        'stagesinfo':[None]*4
    } 
    history[session.get('user_id')][filename] = info
    return {'rootdf': info['info'],'stagesinfo': info['stagesinfo'] }


@app.route('/api/delete/<stagename>/<filename>')
def delete(stagename,filename):

    if filename in history[session.get('user_id')]:
        history[session.get('user_id')].pop(filename)
    
    delete_location(spark, session.get('user_id')+'/'+stagename+'/'+filename)
    return 'sucessful', 200







@app.route('/api/exportfile/<stagename>/<filename>',methods=['GET'])
def exportfile(filename, stagename):
    
    path = stagename+'/'+filename
    src = session.get('user_id')+'/'+path
    dst = session.get('user_id')+'/tmp/'+path
    
    copy_merge_into(spark, src, dst, delete_source=False) # merge 2 a file then delete the merged one
    
    return send_file(dst, as_attachment=True, mimetype='text/csv')


@app.route('/api/exportdf/<filename>/<stage>/<idx>', methods=['GET'])
def exportdf(filename, stage, idx):
    stage = int(stage)-1
    df = history[session.get('user_id')][filename]['stagesdf'][stage]['subdf'][int(idx)]
    
    path = session.get('user_id')+'/tmp/subdf_'+str(stage)+'_'+idx+'_'+filename
    
    app.logger.info(df.dtypes)
    
    for col in df.dtypes:
        df = df.withColumn(col[0],F.col(col[0]).cast('string'))
    
    df.write.csv(path+'.tmp',header=True,mode='overwrite') # ghi xuong hdfs
        
    copy_merge_into(spark, path+'.tmp', path) # merge 2 a file then delete the merged one
    
    
    return send_file(path, as_attachment=True, mimetype='text/csv',)








def getfilelist(src):
    hadoop, conf, fs = configure_hadoop(spark)
    path = hadoop.fs.Path
    files= []
    folders = []
    for x in fs.listStatus(path(src)):
        if x.isFile():
            files.append(x.getPath().toString().split('/')[-1])
        else:
            folders.append(x.getPath().toString().split('/')[-1])
    return [
        src.split('/')[-1],
        folders,
        files
    ]




@app.route('/api/upload', methods=['POST'])
def upload():
    # check if the post request has the file part
    if 'file' not in request.files:
        return redirect(request.url)
        
    file = request.files['file']
    # If the user does not select a file, the browser submits an
    # empty file without a filename.
    if file.filename == '':
        return redirect(request.url)
        
    if file:
        filename = secure_filename(file.filename)
        
        
        file.save(os.path.join(basedir,session['user_id']+'/upload/'+filename))
        
        
        # move uploaded file to hdfs
        hadoop, conf, fs = configure_hadoop(spark)
        fs.moveFromLocalFile(hadoop.fs.Path('./'+session['user_id']+'/upload/'+filename),hadoop.fs.Path(session['user_id']+'/upload/'+filename))
        # ----
        
        columnsmap = json.loads(request.form['columnsmap'])
        app.logger.info(columnsmap)
        app.logger.info(type(columnsmap))
        
        
        delimiter = request.form['delimiter']
        header = request.form['header']
        app.logger.info(header=='true')

        if session.get('user_id') not in history:
            history[session.get('user_id')]={}
            
        app.logger.info(history)
        # [df, name, old] = readdata(filename = filename,header=header,headermap = columnsmap,delimiter=delimiter,user=session['user_id'])
        df, newfilename = readdata(filename = filename,header=(header=='true'),headermap = columnsmap,delimiter=delimiter,user=session['user_id'])
        
        # delete_location(spark,session['user_id']+'/upload/'+filename)
        

        return {
            'rootinfo': history[session['user_id']][newfilename]['info'],
            'stagesinfo':history[session['user_id']][newfilename]['stagesinfo']
        }


@app.route('/api/init',methods=['POST'])
def initstep():
    root = request.form['root'] # name of the file that is processed at the beginning
    app.logger.info(root)
    if root in history[session.get('user_id')]:
        if history[session.get('user_id')][root]['info']['currentstage']['no'] == 0:
            src = history[session.get('user_id')][root]['rootdf']
            k = float(request.form['k'])
            seed = int(request.form['seed'])
            df = kpercent(src,root,str(session.get('user_id')), k , seed)
            
            return {
                'stagesinfo': history[session.get('user_id')][root]['stagesinfo']
            }
    return 'theres no init stage to execute', 401


@app.route('/api/sample',methods=['POST'])
def takesamplestep():
    root = request.form['root'] # name of the file that is processed at the beginning
    app.logger.info(root)
    if root in history[session.get('user_id')]:
        if history[session.get('user_id')][root]['info']['currentstage']['no'] == 0:
            src = history[session.get('user_id')][root]['rootdf']
            k = float(request.form['k'])
            seed = int(request.form['seed'])
            df = kpercent(src,root,str(session.get('user_id')), k , seed)
            
            return {
                'stagesinfo': history[session.get('user_id')][root]['stagesinfo']
            }
    return 'theres no init stage to execute', 401


@app.route('/api/ncore',methods=['POST'])
def ncorestep():
    root = request.form['root'] # name of the file that is processed at the beginning
    app.logger.info(root)
    if root in history[session.get('user_id')]:
        if history[session.get('user_id')][root]['info']['currentstage']['no'] == 1:
            src = history[session.get('user_id')][root]['rootdf']
            filename = history[session.get('user_id')][root]['info']['savedname']
            
        if history[session.get('user_id')][root]['stagesdf'][0]:
            src = history[session.get('user_id')][root]['stagesdf'][0]['maindf']
            filename = history[session.get('user_id')][root]['stagesinfo'][0]['maindf']['savedname']
            
        n = int(request.form['n'])
        
        df = n_core(src, root, filename, str(session.get('user_id')), n)
        
        return {
            'stagesinfo': history[session.get('user_id')][root]['stagesinfo']
        }
    return 'theres no ksample stage to execute', 401

@app.route('/api/mappingid',methods=['POST'])
def mappingstep():
    root = request.form['root'] # name of the file that is processed at the beginning
    app.logger.info(root)
    if root in history[session.get('user_id')]:
        if history[session.get('user_id')][root]['info']['currentstage']['no'] == 2:
            src = history[session.get('user_id')][root]['rootdf']
            filename = history[session.get('user_id')][root]['info']['savedname']
        if history[session.get('user_id')][root]['stagesdf'][1]:
            src = history[session.get('user_id')][root]['stagesdf'][1]['maindf']
            filename = history[session.get('user_id')][root]['stagesinfo'][1]['maindf']['savedname']
        
        df = mappingid(src,root, filename,session.get('user_id'))
        
        return {
            'stagesinfo': history[session.get('user_id')][root]['stagesinfo']
        }
    return 'theres no n core stage to execute', 401


@app.route('/api/split',methods=['POST'])
def splitstep():
    root = request.form['root'] # name of the file that is processed at the beginning
    app.logger.info(root)
    if root in history[session.get('user_id')]:
        if history[session.get('user_id')][root]['info']['currentstage']['no'] == 3:
            src = history[session.get('user_id')][root]['rootdf']
            filename = history[session.get('user_id')][root]['info']['savedname']
        if history[session.get('user_id')][root]['stagesdf'][2]:
            src = history[session.get('user_id')][root]['stagesdf'][2]['maindf']
            filename = history[session.get('user_id')][root]['stagesinfo'][2]['maindf']['savedname']
        
        train = float(request.form['train'])
        valid = float(request.form['valid'])
        test = float(request.form['test'])
        
        limit = float(request.form['limit'])
        
        if request.form['numFeatures'] == 'Default':
            numFeatures = False
        else:
            numFeatures = src.select('item_id').distinct().count()
        
        numHashTables = int(request.form['numHashTables'])
        
        modeltype = request.form['modeltype'] # SB or LB
        
        itemgrp = groupItem(src, modeltype)
        
        splitdf = train_valid_test_split(itemgrp,root,filename,str(session.get('user_id')), train,valid,test, modeltype)
        
        hash, jaccard = jaccardD2(itemgrp, root, filename, session.get('user_id'),limit,numHashTables, numFeatures)
        
        history[session.get('user_id')][root]['stagesdf'][3]['subdf']=[itemgrp, hash, jaccard]
        
        history[session.get('user_id')][root]['stagesinfo'][3]['subdf']=[
            {
                'name': 'Items grouped',
                'html': itemgrp.limit(10).toPandas().to_html()
            },
            {
                'name': 'The min-wise independent permutations locality sensitive hashing scheme',
                'html': hash.limit(10).toPandas().to_html()
            },
            {
                'name': 'Jaccard distance with limit='+str(limit)+' numFeatures='+str(numFeatures)+' numHashTables='+str(numHashTables),
                'html': jaccard.limit(10).toPandas().to_html()
            }
        ]
        
        return {
            'stagesinfo': history[session.get('user_id')][root]['stagesinfo']
        }
    return 'theres no ksample stage to execute', 401
"""
# -------------------------------------------------------
from LocationBasedModel.LightGCN import LightGCN, data_generator, ensureDir, tf, np, args
history = {}
using = {}
config = tf.ConfigProto()
config.gpu_options.allow_growth = True
sess = tf.Session(config=config)
item_batch = range(100)
config = dict()
config['n_users'] = data_generator.n_users
config['n_items'] = data_generator.n_items

interaction_adj, social_adj, similar_users_adj, sharing_adj, location_adj, similar_item_adj, similar_item_time_adj, spatio_temporal_adj = data_generator.get_norm_adj_mat()
config['norm_adj'] = interaction_adj
config['social_adj'] = social_adj
config['similar_users_adj'] = similar_users_adj
config['spatio_temporal_adj'] = spatio_temporal_adj
config['sharing_adj'] = sharing_adj
config['location_adj'] = location_adj
config['similar_item_adj'] = similar_item_adj
config['similar_item_time_adj'] = similar_item_time_adj
model = LightGCN(data_config=config, pretrain_data = None)
saver = tf.train.Saver()

weights_save_path = 'LocationBasedModel/weights/testingmodel/LightGCN'
ensureDir(weights_save_path)
save_saver = tf.train.Saver(max_to_keep=1)

config = tf.ConfigProto()
config.gpu_options.allow_growth = True
sess = tf.Session(config=config)
pretrain_path = 'LocationBasedModel/weights/LB_gowalla_custom/LightGCN'

ckpt = tf.train.get_checkpoint_state(os.path.dirname(pretrain_path + '/checkpoint'))
print(ckpt.model_checkpoint_path)
if ckpt and ckpt.model_checkpoint_path:
    sess.run(tf.global_variables_initializer())
    saver.restore(sess, ckpt.model_checkpoint_path)
    print('load the pretrained model parameters from: ', pretrain_path)

item_batch = range(data_generator.n_items)

@app.route('/api/loadmodel/<modelname>/<foldername>',methods = ['GET','POST'])
def loadmodel(modelname,foldername):

    
    
    return 200

import random


@app.route('/api/interactionlogging/<user>/<itemid>/<top>',methods = ['GET','POST'])
def logging(user,itemid, top):
    users = [int(user)]
    pos_items = [int(itemid)]
    check = True
    while check:
        neg_items = random.randint(0,data_generator.n_items-1)
        if neg_items != pos_items[0]:
            neg_items = [neg_items]
            check = False
    
    data = sess.run(
        [model.opt, model.loss, model.mf_loss, model.emb_loss, model.reg_loss],
        feed_dict={model.users: users, model.pos_items: pos_items,
                   model.node_dropout: eval(args.node_dropout),
                   model.mess_dropout: eval(args.mess_dropout),
                   model.neg_items: neg_items})
    app.logger.info(data)
                   
    rate_batch = sess.run(model.batch_ratings, {model.users: [int(user)],
                                                model.pos_items: item_batch})
    
    predicted = np.argsort(rate_batch)[0][-int(top):]
    result = []
    
    for i in predicted:
        app.logger.info(i)
        app.logger.info(type(i))
        
        result += [
            {'key': int(i), 'location': { 'lat': float(data_generator.poi[i][1]), 'lng': float(data_generator.poi[i][2])  }},
        ]
    print()
    # model.predict
    return {
        'predictresult': result
    }

@app.route('/api/predict/<user>/<top>',methods = ['GET','POST'])
def predict(user,top):
    
    rate_batch = sess.run(model.batch_ratings, {model.users: [int(user)],
                                                model.pos_items: item_batch})
    
    predicted = np.argsort(rate_batch)[0][-int(top):]
    app.logger.info(predicted)
    result = []
    
    for i in predicted:
        app.logger.info(i)
        app.logger.info(type(i))
        
        result += [
            {'key': int(i), 'location': { 'lat': float(data_generator.poi[i][1]), 'lng': float(data_generator.poi[i][2])  }}
        ]
    # model.predict
    return {
        'predictresult': result
    }
@app.route('/api/getlocation/<user>/<top>',methods = ['GET','POST'])
def getlocation(user,top):
    lat = float(request.form['lat'])
    lng = float(request.form['lng'])
    center = [0,lat, lng]
    user = int(user)
    
    difference_array = np.absolute(data_generator.poi-center)
    
    closestlat = np.argwhere(difference_array[:,1]<30).T[0]
    app.logger.info(closestlat.shape)
    
    closest = np.argwhere(difference_array[:,2][closestlat]<10).T[0]
    app.logger.info(closest.shape)
    
    closest = difference_array[closestlat][closest]
    app.logger.info(closest.shape)
    
    indexclose = np.argsort(closest[:,2])[:int(top)]
    app.logger.info(indexclose.shape)
    # find the index of minimum element from the array
    index = closest[indexclose][:,0]
    app.logger.info(index.shape)
    result = []
    
    for i in index:
        app.logger.info(i)
        result += [
            {'key': int(i), 'location': { 'lat': float(data_generator.poi[int(i)][1]), 'lng': float(data_generator.poi[int(i)][2])  }}
        ]
    return {
        'predictresult': result
    }
    
@app.route('/api/traintest/<user>/<top>',methods = ['GET','POST'])
def traintest(user,top):
    user = int(user)
    train = data_generator.train_items[user]
    test = data_generator.test_set[user]
    resulttrain=[]
    resulttest=[]
    for i in train:
        resulttrain += [
            {'key': int(i), 'location': { 'lat': float(data_generator.poi[i][1]), 'lng': float(data_generator.poi[i][2])  }}
        ]
    for i in test:
        resulttest += [
            {'key': int(i), 'location': { 'lat': float(data_generator.poi[i][1]), 'lng': float(data_generator.poi[i][2])  }}
        ]
    
    return {
        'train': resulttrain,
        'test': resulttest
    }
    


    

@app.route('/api/itemsearch/<id>',methods = ['GET','POST'])
def itemsearch(id):
    return {
        'searchresult': [
            {'key': 'operaHouse', 'location': { 'lat': -33.8567844, 'lng': 151.213108  }},
        ],
        'nearby': [
            {'key': 'operaHouse', 'location': { 'lat': -33.8567844, 'lng': 151.213108  }},
        ]
    }

# spark-submit --master spark://192.168.1.4:7077 --conf spark.driver.host=192.168.1.4 server.py

if __name__ == '__main__':
    port = 8081
    app.run(debug = True, port = port)