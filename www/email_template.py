#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-08-03

from gcommon.app import const


class SlimEmailTemplate(object):
    def __init__(self, config):
        self._validate_base_url = config.get('www.accounts.base_url')

        self._invite_validation_url = config.get('www.accounts.invite_validation_api')
        self._forget_password_url = config.get('www.accounts.forget_pwd_api')
        self._verify_email_url = config.get('www.accounts.verify_email_api')

        self.register_mail_subject = config.get('www.sys_mail.register_subject')
        self.invite_mail_subject = config.get('www.sys_mail.invite_subject')
        self.reset_pwd_subject = config.get('www.sys_mail.reset_pwd_subject')

        self.load_templates(config)

    def load_templates(self, config):
        register_confirmation_mail_template = config.get('www.mailer.register_confirmation_mail_template')
        with open(register_confirmation_mail_template, 'r') as f:
            self._confirmation_mail_template = f.read()

        invite_mail_template = config.get('www.mailer.invite_mail_template')
        with open(invite_mail_template, 'r') as f:
            self._invite_mail_template = f.read()

        forget_password_mail_template = config.get('www.mailer.forget_pwd_mail_template')
        with open(forget_password_mail_template, 'r') as f:
            self._forget_password_mail_template = f.read()

        welcome_mail_template = config.get('www.mailer.welcome_mail_template')
        with open(welcome_mail_template, 'r') as f:
            self._welcome_mail_template = f.read()

    def invite_validation_url(self, team_id, email, token_text):
        return self._validate_base_url + self._invite_validation_url % (email, team_id, token_text)

    def forget_password_url(self, email, token_id, token_text):
        return self._validate_base_url + self._forget_password_url % (email, token_id, token_text)

    def email_verify_url(self, email, token):
        return self._validate_base_url + self._verify_email_url % (email, token)

    def render_register_confirmation_mail(self, email, token):
        """ An email with token to verify email address """
        verify_url = self.email_verify_url(email, token)

        mail_content = self._confirmation_mail_template.replace(const.TEMPLATE_ACCOUNT_EMAIL, email.encode('utf-8'))
        mail_content = mail_content.replace(const.TEMPLATE_VALIDATION_URL, verify_url.encode('utf-8'))
        return mail_content

    def render_welcome_mail(self, email, password):
        """ Welcome mail is sent after accepted invitation """
        mail_content = self._welcome_mail_template.replace(const.TEMPLATE_AUTO_PASSWORD, password)
        mail_content = mail_content.replace(const.TEMPLATE_ACCOUNT_EMAIL, email.encode('utf-8'))
        return mail_content

    def render_invitation_email(self, team_id, token_text, inviter_mail, invitee_mail, invitee_alias):
        """ render invite validation email """
        token_text = self._trim_token_text(token_text)

        accept_url = self.invite_validation_url(team_id, invitee_mail, token_text)
        # Template rendering
        mail_content = self._invite_mail_template.replace(const.TEMPLATE_INVITEE, invitee_alias.encode('utf8'))
        mail_content = mail_content.replace(const.TEMPLATE_INVITER, inviter_mail.encode('utf8'))
        mail_content = mail_content.replace(const.TEMPLATE_TRIAL_TOKEN, token_text)
        mail_content = mail_content.replace(const.TEMPLATE_VALIDATION_URL, accept_url.encode('utf8'))
        return str(mail_content)

    def render_reset_password_mail(self, token_id, token_text, email):
        """render reset pwd email"""
        validate_url = self._validate_base_url + self._forget_password_url % (email, token_id, token_text)

        # Template rendering
        mail_content = self._forget_password_mail_template.replace(const.TEMPLATE_VALIDATION_URL, validate_url.encode('utf-8'))
        return str(mail_content)

    @staticmethod
    def _trim_token_text(token_text):
        """ sending uuid shouldn't contains '-' """
        token_text = str(token_text).replace('-', '')
        return token_text

