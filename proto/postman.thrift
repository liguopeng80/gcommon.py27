/* Postman covers apple APNs service */
namespace py slimproto.postman

struct aps_alert
{
    1: optional string title
    2: required string body
    3: optional string title_loc_key
    4: optional list<string> title_loc_args
    5: optional string action_loc_key
    6: optional string loc_key
    7: optional list<string> loc_args
    8: optional string launch_image
}

struct aps_payload
{
    1: required aps_alert alert,
    2: required i32 badge,
    3: optional string sound,
    4: optional i32 content_available,
    5: optional map<string, string> acme
}

service Postman{
    oneway void push_notification(1: string token,
                                  2: aps_payload payload,
                                  3: i64 target_id)

    oneway void send_system_mail(1: string receiver,
                                 2: string subject,
                                 3: string body)
}
