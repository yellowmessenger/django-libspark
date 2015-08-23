'use strict';

describe('Service: request', function () {

  // load the service's module
  beforeEach(module('dashboardUiApp'));

  // instantiate service
  var request;
  beforeEach(inject(function (_request_) {
    request = _request_;
  }));

  it('should do something', function () {
    expect(!!request).toBe(true);
  });

});
