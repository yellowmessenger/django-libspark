'use strict';

/**
 * @ngdoc function
 * @name dashboardUiApp.controller:AboutCtrl
 * @description
 * # AboutCtrl
 * Controller of the dashboardUiApp
 */
angular.module('dashboardUiApp')
  .controller('AboutCtrl', function ($scope) {
    $scope.awesomeThings = [
      'HTML5 Boilerplate',
      'AngularJS',
      'Karma'
    ];
  });
