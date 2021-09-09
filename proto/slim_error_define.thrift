/* error_define.thrift
 * This is auto generated slim error codes
 * Generated on '2016-01-12'
 */

namespace cpp error_define
namespace py slimproto.error_define

enum slim_errors
{

	ok = 0,

	gen_server_internal = 100001,
	gen_bad_request = 100002,
	gen_partial_completed = 100003,
	gen_permission_denied = 100004,
	gen_exceed_request_limit = 100005,
	gen_ip_banned = 100006,

	err_user_exists = 110001,
	reg_too_much_reg_in_current_loc = 110002,
	err_token_invalid = 110003,
	err_token_expired = 110004,
	err_user_inactive = 110005,
	err_bad_token = 110006,
	err_user_banned = 110007,
	err_user_activated = 110008,
	err_user_pending_email_validation = 110009,
	gen_need_authentication = 110065,
	auth_invalid_user_or_pass = 110066,
	auth_too_much_errors = 110067,
	auth_invalid_account_status = 110068,
	auth_permission_denied = 110069,

	auth_invalid_token = 120001,
	auth_invalid_credential = 120002,
	auth_invalid_terminal_id = 120003,
	auth_unsupported_terminal_type = 120004,
	auth_unsupported_OS_type = 120005,

	rpc_server_not_available = 500001,
	rpc_server_suspended = 500002,
	rpc_client_bad_routing = 500003,
	rpc_server_timeout = 500004,
	rpc_server_internal = 500005,

	server_too_much_users = 880001,
	server_not_implemented = 880002,

	gen_client_version_expired = 990001,
	gen_config_version_expired = 990002,
	gen_protocol_not_supported = 990003,

	nexus_invalid_license = 900001,
}

/* End of File */
