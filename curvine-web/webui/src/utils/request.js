/*
 * Copyright 2025 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import axios from 'axios';
import store from '@/store';

/**
 * http request utils
 *
 */

// create an axios instance
const service = axios.create({
    baseURL: process.env.VUE_APP_BASE_API,
    timeout: 10000 // request timeout
});
// request interceptor
service.interceptors.request.use(
    config => {
        // do something before request is sent
        // const token = storage.get("ACCESS_TOKEN")
        // if (token) {
        //     config.headers["Authorization"] = `Bearer ${token}`
        // }
        return config;
    },
    error => {
        // do something with request error
        console.log(error) // for debug
        store.commit('SET_ERRMSG', error)
        Promise.reject(error);
    }
);

// response interceptor
service.interceptors.response.use(
    /**
     * If you want to get http information such as headers or status
     * Please return  response => response
     */

    /**
     * Determine the request status by custom code
     * Here is just an example
     * You can also judge the status by HTTP Status Code
     */
    response => {
        return response;
    },
    error => {
        console.log('request err: ' + error) // for debug
        let errMsg = '';
        if (error && error.response) {
            switch (error.response.status) {
                case 401:
                    errMsg = 'UnAuthorized Request';
                    break;
                case 403:
                    errMsg = 'Request Reject';
                    break;
                case 404:
                    errMsg = 'Request URI Not Found';
                    break;
                case 408:
                    errMsg = 'Request Timeout';
                    break;
                case 500:
                    errMsg = 'Internal service error';
                    break;
                case 501:
                    errMsg = 'Not support service';
                    break;
                case 502:
                    errMsg = 'Gateway Error';
                    break;
                case 503:
                    errMsg = 'Service temporary unavailable';
                    break;
                case 504:
                    errMsg = 'Gateway Timeout';
                    break;
                case 505:
                    errMsg = 'HTTP version not support';
                    break;
                default:
                    errMsg = error.response.data.msg;
                    break;
            }
        } else {
            errMsg = error;
        }
        console.log(errMsg)
        store.commit('SET_ERRMSG', error)
        // Message.error(errMsg);
        return Promise.reject(errMsg);
    }
);
export default service;