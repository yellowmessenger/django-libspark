'use strict';

/**
 * @ngdoc function
 * @name dashboardUiApp.controller:DashboardCtrl
 * @description
 * # DashboardCtrl
 * Controller of the dashboardUiApp
 */
angular.module('dashboardUiApp')
  .controller('DashboardCtrl', function ($scope,request) {
  	$scope.methodOneChecked = [];
  	$scope.endMethodChecked = null;
    $scope.methodTypeOne = [
    	{
    		'name': 'map',
    		'func' : true,
    		'checked': false
    	},

    	{
    		'name': 'distinct',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'filter',
    		'func' : true,
    		'checked': false
    	},
    	{
    		'name': 'flatMap',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'flatMapValues',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'groupBy',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'groupByKey',
    		'func' : false,
    		'checked': false
    	},
		{
    		'name': 'intersection',
    		'func' : false,
    		'checked': false
    	},
		{
    		'name': 'join',
    		'func' : false,
    		'checked': false
    	},
		{
    		'name': 'keyBy',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'keys',
    		'func' : false,
    		'checked': false
    	},
		{
    		'name': 'leftOuterJoin',
    		'func' : false,
    		'checked': false
    	},
		{
    		'name': 'mapPartitions',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'mapPartitionsWithIndex',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'mapPartitionsWithSplit',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'mapValues',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'reduceByKey',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'reduceByKeyLocally',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'rightOuterJoin',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'sortBy',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'sortByKey',
    		'func' : true,
    		'checked': false
    	},
		{
    		'name': 'substract',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'union',
    		'func' : true,
    		'checked': false
    	},
    	{
    		'name': 'values',
    		'func' : true,
    		'checked': false
    	},
    	{
    		'name': 'zip',
    		'func' : true,
    		'checked': false
    	},
    	{
    		'name': 'zipWithIndex',
    		'func' : true,
    		'checked': false
    	},
    	{
    		'name': 'zipWithUniqueId',
    		'func' : true,
    		'checked': false
    	},


    ];

		
    $scope.endMethods = [
    	{
    		'name': 'first',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'collect',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'cache',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'count',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'max',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'mean',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'min',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'name',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'sum',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'take',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'takeOrdered',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'top',
    		'func' : false,
    		'checked': false
    	},
    	{
    		'name': 'variance',
    		'func' : false,
    		'checked': false
    	},
    	

    ];
     function chekedMethodOneList () {
	    $scope.methodOneChecked=[];
	  	$scope.funcList = [];
    	for (var i = 0; i < $scope.methodTypeOne.length; i++) {
    		if($scope.methodTypeOne[i].checked){
    			$scope.methodOneChecked.push($scope.methodTypeOne[i])
    			if($scope.methodTypeOne[i].func){
    				$scope.funcList.push($scope.methodTypeOne[i]);
    			}
    		}
    	};
    }
    $scope.checkMethodOne = function (id) {
    	// $scope.methodTypeOne.checked=false
    	chekedMethodOneList()
    };
    $scope.SendData = function () {
    	var data = [];
    	data[0] = $scope.methodOneChecked;
    	data[1] = $scope.endMethodChecked;
    	data[3] = $scope.data;
    	request.SendData(data).then(function  (res) {
    		$scope.output = res.success
    	})
    }
  });
