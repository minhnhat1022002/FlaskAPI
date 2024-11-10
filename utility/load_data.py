import os
import numpy as np
import random as rd
import scipy.sparse as sp
from datetime import datetime
from time import time
from numpy import ndarray
from math import radians, cos, sin, asin, sqrt
import numpy.linalg as lin


class Data(object):
    def __init__(self, path, batch_size):
        self.path = path
        self.batch_size = batch_size
        self.n_users, self.n_items = 0, 0
        self.n_train, self.n_test = 0, 0
        self.neg_pools = {}
        self.exist_users = []
        self.E = np.array([[0, 0, 1],
                           [0, 1, 0],
                           [-1, 0, 0]])

        self.L = sp.dok_matrix((self.n_items, self.n_items), dtype=np.float32)

        self.poi = np.load(path + '/poi.npy')

        with open(path + '/train.txt') as f:
            for l in f.readlines():
                if len(l) > 0:
                    l = l.strip('\n').split(' ')
                    items = int(l[1])
                    uid = int(l[0])
                    self.exist_users.append(uid)
                    self.n_items = max(self.n_items, items)
                    self.n_users = max(self.n_users, uid)
                    self.n_train += 1

        with open(path + '/test.txt') as f:
            for l in f.readlines():
                if len(l) > 0:
                    l = l.strip('\n').split(' ')
                    try:
                        items = int(l[1])
                    except Exception:
                        continue
                    self.n_items = max(self.n_items, items)
                    self.n_test += 1
        self.n_items += 1
        self.n_users += 1
        self.print_statistics()

        self.R = sp.dok_matrix((self.n_users, self.n_items), dtype=np.float32)
        self.S = sp.dok_matrix((self.n_users, self.n_users), dtype=np.float32)

        self.R1, self.R2, self.R3, self.R4 \
            = (sp.dok_matrix((self.n_users, self.n_items), dtype=np.float32),
               sp.dok_matrix((self.n_users, self.n_items), dtype=np.float32),
               sp.dok_matrix((self.n_users, self.n_items), dtype=np.float32),
               sp.dok_matrix((self.n_users, self.n_items), dtype=np.float32))

        self.train_items, self.test_set, self.user_time_interact, self.item_time_interact = {}, {}, {}, {}
        with open(path + '/train.txt') as f_train:
            with open(path + '/test.txt') as f_test:
                for l in f_train.readlines():
                    if len(l) == 0:
                        break
                    l = l.strip('\n')
                    items = [int(i) for i in l.split(' ')]
                    uid, train_iid, time_interact = items[0], items[1], self.timestamp2hour(items[2])

                    if time_interact == 1:
                        self.R1[uid, train_iid] = 1.
                    elif time_interact == 2:
                        self.R2[uid, train_iid] = 1.
                    elif time_interact == 3:
                        self.R3[uid, train_iid] = 1.
                    else:
                        self.R4[uid, train_iid] = 1.

                    self.R[uid, train_iid] = 1.

                    if uid not in self.train_items:
                        train_items = [train_iid]
                        user_time_interact = [time_interact]
                        self.train_items[uid] = train_items
                        self.user_time_interact[uid] = user_time_interact
                    else:
                        train_items = train_items + [train_iid]
                        user_time_interact = user_time_interact + [time_interact]
                        self.train_items[uid] = train_items
                        self.user_time_interact[uid] = user_time_interact

                    if train_iid not in self.item_time_interact:
                        item_time_interact = [time_interact]
                        self.item_time_interact[train_iid] = item_time_interact
                    else:
                        item_time_interact = self.item_time_interact[train_iid] + [time_interact]
                        self.item_time_interact[train_iid] = item_time_interact

                for l in f_test.readlines():
                    if len(l) == 0:
                        break
                    l = l.strip('\n')
                    try:
                        items = [int(i) for i in l.split(' ')]
                    except Exception:
                        continue
                    uid, test_iid = items[0], items[1]

                    if uid not in self.test_set:
                        test_items = [test_iid]
                        self.test_set[uid] = test_items
                    else:
                        test_items = test_items + [test_iid]
                        self.test_set[uid] = test_items

        self.ST = sp.dok_matrix((self.n_users, self.n_items), dtype=np.float32)

        self.user_location, self.item_time = {}, {}
        for i in range(self.n_users):
            user_location = self.get_centroid(
                map(lambda x: [self.poi[int(x)][2], self.poi[int(x)][1]], self.train_items[i]))
            user_time = sum(self.user_time_interact[i]) / len(self.user_time_interact[i])
            for j in self.train_items[i]:
                spatio = self.haversine(lon1=user_location[0], lat1=user_location[1], lon2=self.poi[int(j)][0],
                                        lat2=self.poi[int(j)][1])
                temporal = 4 - abs(user_time - (sum(self.item_time_interact[j]) / len(self.item_time_interact[j])))
                if spatio <= 100:
                    if temporal <= 1:
                        continue
                    elif 1 < temporal <= 2:
                        self.ST[i, j] = 1
                    elif 2 < temporal <= 3:
                        self.ST[i, j] = 10
                    else:
                        self.ST[i, j] = 100


        self.U = self.R.dot(self.R.transpose())
        self.I = self.R.transpose().dot(self.R)
        self.I1 = self.R1.transpose().dot(self.R1)
        self.I2 = self.R2.transpose().dot(self.R2)
        self.I3 = self.R3.transpose().dot(self.R3)
        self.I4 = self.R4.transpose().dot(self.R4)

        self.n_social = 0
        if os.path.exists(path + '/social_trust.txt'):
            with open(path + '/social_trust.txt') as f_social:
                for l in f_social.readlines():
                    if len(l) == 0:
                        break
                    l = l.strip('\n')
                    users = [int(i) for i in l.split(' ')]
                    uid, friends = users[0], users[1]
                    self.S[uid, friends] = 1.
                    self.n_social = self.n_social + 1

    def timestamp2hour(self, timestamp):
        def convert(v):
            if v <= 6:
                return 1
            elif 6 < v <= 12:
                return 2
            elif 12 < v <= 18:
                return 3
            else:
                return 4

        d = datetime.fromtimestamp(timestamp)
        return convert(d.hour)

    def get_norm_adj_mat(self):
        def normalized_sym(adj): # Ma tran D nam o day
            """
            Â = D^(-1/2)*A*D^(-1/2)
            adj: matrix A
            d_mat_inv: matrix D
            norm_adj: Â
            """
            rowsum: ndarray = np.array(adj.sum(1)) # Dem so tuong tac tung user (the hien bac cua mot dinh)
            d_inv = np.power(rowsum, -0.5).flatten()  # lam phang ma tran thu duoc lan luot bac cua tung dinh
            d_inv[np.isinf(d_inv)] = 0. # xoa di phan tu co gia tri inf
            d_mat_inv = sp.diags(d_inv) # tao ma tran duong cheo chua so bac cua moi dinh

            norm_adj = d_mat_inv.dot(adj) #Â = D^(-1/2)*A
            norm_adj = norm_adj.dot(d_mat_inv)# Â = Â*D^(-1/2)
            return norm_adj.tocsr()

        try:
            t1 = time()
            interaction_adj_mat_sym = sp.load_npz(self.path + '/s_interaction_adj_mat.npz')
            print('already load interaction adj matrix', interaction_adj_mat_sym.shape, time() - t1)
        except Exception:
            interaction_adj_mat = self.create_interaction_adj_mat()
            interaction_adj_mat_sym = normalized_sym(interaction_adj_mat)
            print('generate symmetrically normalized interaction adjacency matrix.')
            sp.save_npz(self.path + '/s_interaction_adj_mat.npz', interaction_adj_mat_sym)

        try:
            t2 = time()
            social_adj_mat_sym = sp.load_npz(self.path + '/s_social_adj_mat.npz')
            print('already load social adj matrix', social_adj_mat_sym.shape, time() - t2)
        except Exception:
            social_adj_mat = self.create_social_adj_mat()
            social_adj_mat_sym = normalized_sym(social_adj_mat)
            print('generate symmetrically normalized social adjacency matrix.')
            sp.save_npz(self.path + '/s_social_adj_mat.npz', social_adj_mat_sym)
        try:
            t3 = time()
            similar_users_adj_mat_sym = sp.load_npz(self.path + '/s_similar_users_adj_mat.npz')
            print('already load similar users adj matrix', similar_users_adj_mat_sym.shape, time() - t3)
        except Exception:
            similar_users_adj_mat = self.create_similar_adj_mat()
            similar_users_adj_mat_sym = normalized_sym(similar_users_adj_mat)
            print('generate symmetrically normalized similar users adjacency matrix.')
            sp.save_npz(self.path + '/s_similar_users_adj_mat.npz', similar_users_adj_mat_sym)

        try:
            t4 = time()
            sharing_adj_mat_sym = sp.load_npz(self.path + '/sharing_adj_mat_sym.npz')
            print('already load sharing users adj matrix', sharing_adj_mat_sym.shape, time() - t4)
        except Exception:
            sharing_adj_mat = self.create_sharing_adj_mat()
            sharing_adj_mat_sym = normalized_sym(sharing_adj_mat)
            print('generate symmetrically normalized sharing adjacency matrix.')
            sp.save_npz(self.path + '/sharing_adj_mat_sym.npz', sharing_adj_mat_sym)

        try:
            t5 = time()
            location_adj_mat_sym = sp.load_npz(self.path + '/location_adj_mat_sym.npz')
            print('already load location adj matrix', location_adj_mat_sym.shape, time() - t5)
        except Exception:
            location_adj_mat = self.rs_mat2s(self.poi, self.n_items)
            location_adj_mat_sym = normalized_sym(location_adj_mat)
            print('generate symmetrically normalized location adjacency matrix.')
            sp.save_npz(self.path + '/location_adj_mat_sym.npz', location_adj_mat_sym)

        try:
            t6 = time()
            similar_item_adj_mat_sym = sp.load_npz(self.path + '/s_similar_item_adj_mat_sym.npz')
            print('already load similar item adj matrix', similar_item_adj_mat_sym.shape, time() - t6)
        except Exception:
            similar_item_adj_mat = self.create_item_similar_adj_mat(self.I)
            similar_item_adj_mat_sym = normalized_sym(similar_item_adj_mat)
            print('generate symmetrically normalized similar users adjacency matrix.')
            sp.save_npz(self.path + '/s_similar_item_adj_mat_sym.npz', similar_item_adj_mat_sym)

        try:
            t7 = time()
            similar_item_time_adj_mat_sym = sp.load_npz(self.path + '/s_similar_item_by_time_adj_mat_sym.npz')
            print('already load similar item (time) adj matrix', similar_item_time_adj_mat_sym.shape, time() - t7)
        except Exception:
            similar_item_time_adj_mat = self.create_item_similar_time_adj_mat()
            similar_item_time_adj_mat_sym = normalized_sym(similar_item_time_adj_mat)
            print('generate symmetrically normalized similar item (time) adjacency matrix.')
            sp.save_npz(self.path + '/s_similar_item_by_time_adj_mat_sym.npz', similar_item_time_adj_mat_sym)

        try:
            t8 = time()
            spatio_temporal_adj_mat_sym = sp.load_npz(self.path + '/s_spatio_temporal_adj_mat.npz')
            print('already load spatio-temporal adj matrix', spatio_temporal_adj_mat_sym.shape, time() - t8)
        except Exception:
            spatio_temporal_adj_mat = self.create_spatio_temporal_adj_mat()
            spatio_temporal_adj_mat_sym = normalized_sym(spatio_temporal_adj_mat)
            print('generate symmetrically normalized spatio-temporal adjacency matrix.')
            sp.save_npz(self.path + '/s_spatio_temporal_adj_mat.npz', spatio_temporal_adj_mat_sym)

        return interaction_adj_mat_sym, social_adj_mat_sym, similar_users_adj_mat_sym, sharing_adj_mat_sym, location_adj_mat_sym, similar_item_adj_mat_sym, similar_item_time_adj_mat_sym, spatio_temporal_adj_mat_sym

    def create_interaction_adj_mat(self):
        # 1. Create Graph Users-Items  Interaction Adjacency Matrix.
        t1 = time()
        interaction_adj_mat = sp.dok_matrix((self.n_users + self.n_items, self.n_users + self.n_items),
                                            dtype=np.float32)
        interaction_adj_mat = interaction_adj_mat.tolil()
        R = self.R.tolil()
        for i in range(5):
            interaction_adj_mat[int(self.n_users * i / 5.0):int(self.n_users * (i + 1.0) / 5), self.n_users:] = \
                R[int(self.n_users * i / 5.0):int(self.n_users * (i + 1.0) / 5)]
            interaction_adj_mat[self.n_users:, int(self.n_users * i / 5.0):int(self.n_users * (i + 1.0) / 5)] = \
                R[int(self.n_users * i / 5.0):int(self.n_users * (i + 1.0) / 5)].T
        print('already create interaction adjacency matrix', interaction_adj_mat.shape, time() - t1)
        return interaction_adj_mat.tocsr()

    def create_social_adj_mat(self):
        # 2. Create Graph Users-Users Social Adjacency Matrix.
        t2 = time()
        social_adj_mat = self.S
        print('already create social adjacency matrix', social_adj_mat.shape, 'social_interactons:', self.n_social,
              time() - t2)
        return social_adj_mat.tocsr()

    def create_similar_adj_mat(self):
        t3 = time()
        similar_users_adj_mat = sp.dok_matrix((self.n_users, self.n_users), dtype=np.float32)

        def cluster(x, d, t):
            v = x / (t + d - x)
            if v <= 0.09:
                return 0
            elif 0.09 < v <= 0.39:
                return 1
            elif 0.49 < v <= 0.69:
                return 10
            elif 0.69 < v <= 0.79:
                return 100
            else:
                return 200

        X = self.U.toarray()
        vfunc = np.vectorize(cluster)

        diag: ndarray = X.diagonal()
        for i in range(X.shape[0]):
            tmp = vfunc(X[i], diag, diag[i])
            similar_users_adj_mat[i] = tmp
        print('already create similar users adjacency matrix', similar_users_adj_mat.shape, time() - t3)
        similar_users_adj_mat = sp.csr_matrix(similar_users_adj_mat)

        return similar_users_adj_mat.tocsr()

    def create_sharing_adj_mat(self):
        # Create Sharing matrix in  SEPT
        def intersect_matrices(mat1, mat2):
            mat1 = np.where((mat1 > 0), 1, 0)
            mat2 = np.where((mat2 > 0), 1, 0)
            if not (mat1.shape == mat2.shape):
                return False
            mat_intersect = np.where((mat1 == mat2), mat1, 0)
            return mat_intersect

        t4 = time()
        sharing_adj_mat = intersect_matrices(self.U.toarray(), self.S.toarray())
        print(np.sum(sharing_adj_mat))
        sharing_adj_mat = sp.csr_matrix(sharing_adj_mat)
        print('already create sharing matrix', sharing_adj_mat.shape, time() - t4)

        return sharing_adj_mat.tocsr()

    def rs_mat2s(self, poi, l_max):
        t5 = time()

        # convert ver1
        # def convert(v):
        #     if v <= 10:
        #         return 200
        #     elif 10 < v <= 30:
        #         return 100
        #     elif 30 < v <= 100:
        #         return 10
        #     elif 100 < v <= 300:
        #         return 1
        #     else:
        #         return 0

        # convert ver2
        def convert(v):
            if v <= 1:
                return 1
            elif 1 < v <= 10:
                return 0.5
            elif 10 < v <= 30:
                return 0.05
            elif 30 < v <= 100:
                return 0.005
            else:
                return 0

        candidate_loc = np.linspace(0, l_max - 1, l_max)  # (L)
        mat = np.zeros((l_max, l_max))  # mat (L, L)
        for i, loc1 in enumerate(candidate_loc):
            for j, loc2 in enumerate(candidate_loc):
                poi1, poi2 = poi[int(loc1)], poi[int(loc2)]  # retrieve poi by loc_id
                tmp = self.haversine(lon1=poi1[2], lat1=poi1[1], lon2=poi2[2], lat2=poi2[1])
                mat[i, j] = convert(tmp)
        mat = sp.csr_matrix(mat)
        print('already create location matrix', mat.shape, time() - t5)
        return mat  # (L, L)

    def create_item_similar_adj_mat(self, I):
        t3 = time()
        similar_item_adj_mat = sp.dok_matrix((self.n_items, self.n_items), dtype=np.float32)

        def cluster(x, d, t):
            v = x / (t + d - x)
            if v <= 0.09:
                return 0
            elif 0.09 < v <= 0.39:
                return 1
            elif 0.49 < v <= 0.69:
                return 10
            elif 0.69 < v <= 0.79:
                return 100
            else:
                return 200

        X = I.toarray()
        vfunc = np.vectorize(cluster)

        diag: ndarray = X.diagonal()
        for i in range(X.shape[0]):
            tmp = vfunc(X[i], diag, diag[i])
            similar_item_adj_mat[i] = tmp
        print('already create similar items adjacency matrix', similar_item_adj_mat.shape, time() - t3)
        similar_item_adj_mat = sp.csr_matrix(similar_item_adj_mat)

        return similar_item_adj_mat.tocsr()

    def create_item_similar_time_adj_mat(self):
        X1 = self.create_item_similar_adj_mat(self.I1)
        X2 = self.create_item_similar_adj_mat(self.I2)
        X3 = self.create_item_similar_adj_mat(self.I3)
        X4 = self.create_item_similar_adj_mat(self.I4)

        X = X1 + X2 + X3 + X4

        return X

    def create_spatio_temporal_adj_mat(self):
        # 1. Create Graph Users-Items  Interaction Adjacency Matrix.
        t1 = time()
        spatio_temporal_adj_mat = sp.dok_matrix((self.n_users + self.n_items, self.n_users + self.n_items),
                                                dtype=np.float32)
        spatio_temporal_adj_mat = spatio_temporal_adj_mat.tolil()
        R = self.ST.tolil()
        for i in range(5):
            spatio_temporal_adj_mat[int(self.n_users * i / 5.0):int(self.n_users * (i + 1.0) / 5), self.n_users:] = \
                R[int(self.n_users * i / 5.0):int(self.n_users * (i + 1.0) / 5)]
            spatio_temporal_adj_mat[self.n_users:, int(self.n_users * i / 5.0):int(self.n_users * (i + 1.0) / 5)] = \
                R[int(self.n_users * i / 5.0):int(self.n_users * (i + 1.0) / 5)].T
        print('already create spatio-temporal adjacency matrix', spatio_temporal_adj_mat.shape, time() - t1)
        return spatio_temporal_adj_mat.tocsr()

    def haversine(self, lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371
        return c * r

    def lat_long2n_E(self, latitude, longitude):
        res = [np.sin(np.deg2rad(latitude)),
               np.sin(np.deg2rad(longitude)) * np.cos(np.deg2rad(latitude)),
               -np.cos(np.deg2rad(longitude)) * np.cos(np.deg2rad(latitude))]
        return np.dot(self.E.T, np.array(res))

    def n_E2lat_long(self, n_E):
        n_E = np.dot(self.E, n_E)
        longitude = np.arctan2(n_E[1], -n_E[2]);
        equatorial_component = np.sqrt(n_E[1] ** 2 + n_E[2] ** 2);
        latitude = np.arctan2(n_E[0], equatorial_component);
        return np.rad2deg(latitude), np.rad2deg(longitude)

    def get_centroid(self, coords):
        res = []
        for lat, lon in coords:
            res.append(self.lat_long2n_E(lat, lon))
        res = np.array(res)
        m = np.mean(res, axis=0)
        m = m / lin.norm(m)
        return self.n_E2lat_long(m)

    def sample(self):
        if self.batch_size <= self.n_users:
            users = rd.sample(self.exist_users, self.batch_size)
        else:
            users = [rd.choice(self.exist_users) for _ in range(self.batch_size)]

        def sample_pos_items_for_u(u, num):
            pos_items = self.train_items[u] # ground truth
            n_pos_items = len(pos_items) # no item
            pos_batch = []
            while True:
                if len(pos_batch) == num:
                    break
                pos_id = np.random.randint(low=0, high=n_pos_items, size=1)[0]
                pos_i_id = pos_items[pos_id]

                if pos_i_id not in pos_batch:
                    pos_batch.append(pos_i_id)
            return pos_batch

        def sample_neg_items_for_u(u, num):
            neg_items = []
            while True:
                if len(neg_items) == num:
                    break
                neg_id = np.random.randint(low=0, high=self.n_items, size=1)[0]
                if neg_id not in self.train_items[u] and neg_id not in neg_items:
                    neg_items.append(neg_id)
            return neg_items

        def sample_neg_items_for_u_from_pools(u, num):
            neg_items = list(set(self.neg_pools[u]) - set(self.train_items[u]))
            return rd.sample(neg_items, num)

        pos_items, neg_items = [], []
        for u in users:
            pos_items += sample_pos_items_for_u(u, 1)
            neg_items += sample_neg_items_for_u(u, 1)

        return users, pos_items, neg_items

    def sample_test(self):
        if self.batch_size <= self.n_users:
            users = rd.sample(self.test_set.keys(), self.batch_size)
        else:
            users = [rd.choice(self.exist_users) for _ in range(self.batch_size)]

        def sample_pos_items_for_u(u, num):
            pos_items = self.test_set[u]
            n_pos_items = len(pos_items)
            pos_batch = []
            while True:
                if len(pos_batch) == num:
                    break
                pos_id = np.random.randint(low=0, high=n_pos_items, size=1)[0]
                pos_i_id = pos_items[pos_id]

                if pos_i_id not in pos_batch:
                    pos_batch.append(pos_i_id)
            return pos_batch

        def sample_neg_items_for_u(u, num):
            neg_items = []
            while True:
                if len(neg_items) == num:
                    break
                neg_id = np.random.randint(low=0, high=self.n_items, size=1)[0]
                if neg_id not in (self.test_set[u] + self.train_items[u]) and neg_id not in neg_items:
                    neg_items.append(neg_id)
            return neg_items

        def sample_neg_items_for_u_from_pools(u, num):
            neg_items = list(set(self.neg_pools[u]) - set(self.train_items[u]))
            return rd.sample(neg_items, num)

        pos_items, neg_items = [], []
        for u in users:
            pos_items += sample_pos_items_for_u(u, 1)
            neg_items += sample_neg_items_for_u(u, 1)

        return users, pos_items, neg_items

    def get_num_users_items(self):
        return self.n_users, self.n_items

    def print_statistics(self):
        print('n_users=%d, n_items=%d' % (self.n_users, self.n_items))
        print('n_interactions=%d' % (self.n_train + self.n_test))
        print('n_train=%d, n_test=%d, sparsity=%.5f' % (
            self.n_train, self.n_test, (self.n_train + self.n_test) / (self.n_users * self.n_items)))
