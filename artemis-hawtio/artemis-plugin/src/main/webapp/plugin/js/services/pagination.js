/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var Artemis;
(function (Artemis) {

    Artemis._module.factory('pagination',
        function () {
            return {
                pageSizeOptions: [10,20,50,100],
                pageSize: 10,
                pageNumber: 1,
                totalItems: 1,
                firstItem: 1,
                lastItem: 1,
                pages: 1,
                page: function (resultsSize) {
                   this.totalItems = resultsSize;
                   this.firstItem = this.pageNumber * this.pageSize - this.pageSize + 1;
                   this.lastItem = this.firstItem + this.pageSize - 1;
                   if (this.lastItem > this.totalItems) {
                       this.lastItem = this.totalItems;
                   }
                   this.pages = Math.ceil(this.totalItems/this.pageSize);
                },
                nextPage: function () {
                   this.pageNumber++;
                   this.load();
                },
                previousPage: function () {
                   this.pageNumber--;
                   this.load();
                },
                lastPage: function () {
                   this.pageNumber = this.pages;
                   this.load();
                },
                firstPage: function () {
                   this.pageNumber = 1;
                   this.load();
                },
                reset: function () {
                   this.pageNumber = 1;
                },
                load: function () {},
                setOperation: function (operation) {
                   this.load = operation;
                }
            }
           
       }).run(configurePagination);

       function configurePagination($templateCache) {
           $templateCache.put('plugin/artemispagination.html',
                `
                <div ng_show="$ctrl.pagination.totalItems > 0">
                    <form class="content-view-pf-pagination table-view-pf-pagination clearfix" id="pagination1">
                       <div class="form-group">
                           <select ng-model="$ctrl.pagination.pageSize" ng-options="qn for qn in $ctrl.pagination.pageSizeOptions" id="pagination.values.pageSize">
                           </select>
                           <span>per page</span>
                       </div>
                       <div class="form-group">
                           <span><span class="pagination-pf-items-current">{{$ctrl.pagination.firstItem}}-{{$ctrl.pagination.lastItem}}</span> of <span class="pagination-pf-items-total">{{$ctrl.pagination.totalItems}}</span></span>
                           <ul class="pagination pagination-pf-back">
                               <li class="{{$ctrl.pagination.pageNumber == 1 ? 'disabled' : ''}}"><a href="#" title="First Page" ng-click="$ctrl.pagination.pageNumber != 1  && $ctrl.pagination.firstPage()"><span class="i fa fa-angle-double-left"></span></a></li>
                               <li class="{{$ctrl.pagination.pageNumber == 1 ? 'disabled' : ''}}"><a href="#" title="Previous Page" ng-click="$ctrl.pagination.pageNumber != 1 && $ctrl.pagination.previousPage()"><span class="i fa fa-angle-left"></span></a></li>
                           </ul>
                           <label for="pagination1-page" class="sr-only">Current Page</label>
                           <input class="pagination-pf-page" type="text" value="{{$ctrl.pagination.pageNumber}}" id="pagination1-page" on-change="$ctrl.firstPage()"/>
                           <span>of <span class="pagination-pf-pages">{{$ctrl.pagination.pages}}</span></span>
                           <ul class="pagination pagination-pf-forward">
                               <li class="{{$ctrl.pagination.pageNumber == $ctrl.pagination.pages ? 'disabled' : ''}}"><a href="#" title="Next Page" ng-click="$ctrl.pagination.pageNumber != $ctrl.pagination.pages && $ctrl.pagination.nextPage()"><span class="i fa fa-angle-right"></span></a></li>
                               <li class="{{$ctrl.pagination.pageNumber == $ctrl.pagination.pages ? 'disabled' : ''}}"><a href="#" title="Last Page" ng-click="$ctrl.pagination.pageNumber != $ctrl.pagination.pages && $ctrl.pagination.lastPage()"><span class="i fa fa-angle-double-right"></span></a></li>
                           </ul>
                       </div>
                    </form>
                </div>
               `
           )
       }
       configurePagination.$inject = ['$templateCache'];

})(Artemis || (Artemis = {}));