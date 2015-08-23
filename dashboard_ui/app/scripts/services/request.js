'use strict';

/**
 * @ngdoc service
 * @name dashboardUiApp.request
 * @description
 * # request
 * Factory in the dashboardUiApp.
 */
angular.module('dashboardUiApp')
  .factory('request', function ($http) {
    var services = {
      SendData: function (data) {
        return $http.post('http://localhost:8000/',data).then(function (response) {
          return response.data;
         });
      }
    }

    // Public API here
    return services;

  });
