'use strict';

/**
 * @ngdoc function
 * @name dashboardUiApp.controller:MainCtrl
 * @description
 * # MainCtrl
 * Controller of the dashboardUiApp
 */
angular.module('dashboardUiApp')
  .controller('MainCtrl', function ($scope) {
    $scope.awesomeThings = [
      'HTML5 Boilerplate',
      'AngularJS',
      'Karma'
    ];
  });
