import React from 'react';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import test from 'node:test';
import { renderToStaticMarkup } from 'react-dom/server';
import ts from 'typescript';

const dir = import.meta.dirname;
const transpiled = ts.transpileModule(readFileSync(join(dir, 'plugin-ui.ts'), 'utf8'), {
  compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 },
}).outputText;
const ui = await import(`data:text/javascript;base64,${Buffer.from(transpiled).toString('base64')}`);

const origin = 'http://localhost:5173';

test('string titles stay unchanged and object titles follow language, en, then key', () => {
  assert.equal(ui.resolvePluginTitle('Overview', 'zh', 'main'), 'Overview');
  assert.equal(ui.resolvePluginTitle('Overview', 'en', 'main'), 'Overview');
  const title = { en: 'Stats', 'zh-CN': '统计' };
  assert.equal(ui.resolvePluginTitle(title, 'zh-CN', 'stats'), '统计');
  assert.equal(ui.resolvePluginTitle(title, 'zh', 'stats'), '统计');
  assert.equal(ui.resolvePluginTitle(title, 'fr', 'stats'), 'Stats');
  assert.equal(ui.resolvePluginTitle({ fr: 'Statistiques' }, 'de', 'stats'), 'stats');
  assert.equal(ui.resolvePluginTitle({ fr: 'Statistiques' }, 'fr', 'stats'), 'Statistiques');
  assert.equal(ui.resolvePluginTitle(null, 'en', 'stats'), 'stats');
});

test('site paths accept one slash and reject protocol-relative, backslash, and control characters', () => {
  assert.equal(ui.isPluginSitePath('/plugins/acme.guard/pages/main', origin), true);
  assert.equal(ui.isPluginSitePath('/dashboard?tab=1#top', origin), true);
  assert.equal(ui.isPluginSitePath('//evil.example/phish', origin), false);
  assert.equal(ui.isPluginSitePath('/\\\\evil.example', origin), false);
  assert.equal(ui.isPluginSitePath('/foo\\bar', origin), false);
  assert.equal(ui.isPluginSitePath('/has\nbreak', origin), false);
  assert.equal(ui.isPluginSitePath('/has\u007fdel', origin), false);
  assert.equal(ui.isPluginSitePath('https://evil.example/dashboard', origin), false);
  assert.equal(ui.isPluginSitePath('dashboard', origin), false);
  assert.equal(ui.isPluginSitePath('', origin), false);
});

test('bridge replies match protocol, version, and the current session', () => {
  const hello = { protocol: ui.PLUGIN_UI_PROTOCOL, version: ui.PLUGIN_UI_VERSION, type: 'hello' };
  assert.deepEqual(ui.matchPluginBridgeMessage(hello, null), { type: 'hello' });
  assert.equal(ui.matchPluginBridgeMessage({ ...hello, version: 2 }, 's1'), null);
  assert.equal(ui.matchPluginBridgeMessage({ ...hello, protocol: 'other' }, 's1'), null);

  const call = {
    protocol: ui.PLUGIN_UI_PROTOCOL,
    version: ui.PLUGIN_UI_VERSION,
    type: 'call',
    sessionId: 's1',
    id: 'c1',
    method: 'graphql',
    params: { query: '{ me { id } }', pluginID: 'ignored' },
  };
  assert.deepEqual(ui.matchPluginBridgeMessage(call, 's1'), {
    type: 'call',
    id: 'c1',
    method: 'graphql',
    params: call.params,
  });
  assert.equal(ui.matchPluginBridgeMessage(call, 'other'), null);
  assert.equal(ui.matchPluginBridgeMessage(call, null), null);
  assert.deepEqual(ui.matchPluginBridgeMessage({ ...call, params: [] }, 's1'), {
    type: 'call',
    id: 'c1',
    method: 'graphql',
    params: null,
  });

  const pageError = {
    protocol: ui.PLUGIN_UI_PROTOCOL,
    version: ui.PLUGIN_UI_VERSION,
    type: 'pageError',
    sessionId: 's1',
    payload: { message: 'boom' },
  };
  assert.deepEqual(ui.matchPluginBridgeMessage(pageError, 's1'), { type: 'pageError', message: 'boom' });
  assert.equal(ui.matchPluginBridgeMessage(pageError, 'other'), null);
  assert.equal(ui.matchPluginBridgeMessage({ protocol: ui.PLUGIN_UI_PROTOCOL, version: 1, type: 'reply', sessionId: 's1' }, 's1'), null);
});

test('theme resolution and plugin HTTP errors stay stable', () => {
  assert.equal(ui.resolvePluginTheme('system', true), 'dark');
  assert.equal(ui.resolvePluginTheme('system', false), 'light');
  assert.equal(ui.resolvePluginTheme('light', true), 'light');
  assert.equal(ui.pluginProjectId('gid://axonhub/Project/12'), 12);
  assert.equal(ui.pluginProjectId(null), null);
  assert.deepEqual(ui.pluginHTTPBridgeError(401), { code: 'UNAUTHENTICATED', message: 'Unauthenticated', status: 401 });
  assert.deepEqual(ui.pluginHTTPBridgeError(403), { code: 'FORBIDDEN', message: 'Forbidden', status: 403 });
  assert.deepEqual(ui.pluginHTTPBridgeError(404), { code: 'NOT_FOUND', message: 'Not found', status: 404 });
  assert.deepEqual(ui.pluginHTTPBridgeError(429), { code: 'RATE_LIMITED', message: 'Rate limited', status: 429 });
  assert.equal(ui.pluginHTTPBridgeError(500).code, 'INTERNAL');
  assert.equal(ui.pluginHTTPBridgeError(500).message, 'Internal error');

  assert.deepEqual(ui.pluginFetchHTTPError(401, null), { code: 'UNAUTHENTICATED', message: 'Unauthenticated', status: 401 });
  assert.deepEqual(ui.pluginFetchHTTPError(403, { code: 'FORBIDDEN' }), { code: 'FORBIDDEN', message: 'Forbidden', status: 403 });
  assert.deepEqual(ui.pluginFetchHTTPError(404, null), { code: 'NOT_FOUND', message: 'Not found', status: 404 });
  assert.equal(ui.pluginFetchHTTPError(429, null).code, 'INTERNAL');
  assert.deepEqual(ui.pluginFetchHTTPError(504, { code: 'TIMEOUT' }), { code: 'TIMEOUT', message: 'Timed out', status: 504 });
  assert.deepEqual(ui.pluginFetchHTTPError(502, null), { code: 'NETWORK_ERROR', message: 'Network error', status: 502 });
  assert.equal(ui.pluginFetchHTTPError(500, { code: 'INTERNAL' }).code, 'INTERNAL');
  assert.deepEqual(ui.pluginFetchHTTPError(409, { code: 'INVALID_STATE', message: 'redirect' }), {
    code: 'INVALID_STATE',
    message: 'Invalid state',
    status: 409,
  });
  assert.equal(ui.pluginFetchHTTPError(504, { code: 'INVALID_STATE' }).code, 'INVALID_STATE');
});

test('plugin fetch calls keep outbound fields and accept a non-2xx result', () => {
  assert.equal(ui.pluginFetchCallParams(null), null);
  assert.equal(ui.pluginFetchCallParams({ url: '' }), null);
  assert.equal(ui.pluginFetchCallParams({ url: 'https://example.com/v1', method: 'get' }), null);
  assert.equal(ui.pluginFetchCallParams({ url: 'https://example.com/v1', headers: { accept: 1 } }), null);
  assert.equal(ui.pluginFetchCallParams({ url: 'https://example.com/v1', headers: ['accept'] }), null);
  assert.equal(ui.pluginFetchCallParams({ url: 'https://example.com/v1', body: { text: 'no' } }), null);
  assert.deepEqual(
    ui.pluginFetchCallParams({
      url: 'https://example.com/v1/check',
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
      pluginId: 'ignored',
    }),
    {
      url: 'https://example.com/v1/check',
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{}',
    }
  );
  assert.deepEqual(ui.pluginFetchResult({ status: 403, headers: { 'content-type': 'text/plain' }, body: 'no' }), {
    status: 403,
    headers: { 'content-type': 'text/plain' },
    body: 'no',
  });
  assert.equal(ui.pluginFetchResult({ status: 200, headers: { ok: 1 }, body: '' }), null);
  assert.equal(ui.pluginFetchResult({ status: 200, body: '' }), null);
});

test('computed style tokens keep the bridge whitelist and drop unsafe values', () => {
  const bridge = readFileSync(join(dir, '../../../../internal/server/plugin/uisdk/bridge.js'), 'utf8');
  const listed = bridge.match(/var TOKEN_NAMES = \[([\s\S]*?)\];/);
  assert.ok(listed);
  const bridgeNames = [...listed[1].matchAll(/"(--[^"]+)"/g)].map((match) => match[1]);
  assert.deepEqual(bridgeNames, [...ui.PLUGIN_UI_TOKEN_NAMES]);

  const style = {
    getPropertyValue(name) {
      const values = {
        '--background': '  oklch(0.16 0.03 264.695) ',
        '--foreground': '#f5f5f4',
        '--card': 'red; color: blue',
        '--radius': '0.75rem',
        '--font-sans': 'Inter, ui-sans-serif, system-ui, sans-serif',
        '--sidebar': '#111111',
        '--muted': 'bad\nvalue',
      };
      return values[name] ?? '';
    },
  };
  assert.deepEqual(ui.readPluginUITokens(style), {
    '--background': 'oklch(0.16 0.03 264.695)',
    '--foreground': '#f5f5f4',
    '--radius': '0.75rem',
    '--font-sans': 'Inter, ui-sans-serif, system-ui, sans-serif',
  });
});

test('plugin page copy exists in en and zh-CN', () => {
  const en = JSON.parse(readFileSync(join(dir, '../../locales/en/plugins.json'), 'utf8'));
  const zh = JSON.parse(readFileSync(join(dir, '../../locales/zh-CN/plugins.json'), 'utf8'));
  for (const key of [
    'plugins.nav.group',
    'plugins.page.notFound',
    'plugins.page.error',
    'plugins.page.retry',
    'plugins.grant.network',
    'plugins.grant.networkWarning',
    'plugins.grant.new',
    'plugins.fields.grantedNetwork',
    'plugins.grant.addedSchedules',
    'plugins.grant.addedRequiredCapabilities',
    'plugins.grant.keySuspensionWarning',
    'plugins.grant.keySuspensionAccess',
    'plugins.grant.usageMetricsHint',
    'plugins.config.ownerOnly',
    'plugins.automation.title',
    'plugins.automation.schedules',
    'plugins.automation.scheduleInterval',
    'plugins.automation.requiredCapabilities',
    'plugins.automation.scope',
    'plugins.automation.scopeHint',
    'plugins.automation.scopeDenied',
    'plugins.automation.project',
    'plugins.automation.allKeys',
    'plugins.automation.selectedKeys',
    'plugins.automation.noKeys',
    'plugins.automation.excludedKeys',
  ]) {
    assert.equal(typeof en[key], 'string', `Missing English translation: ${key}`);
    assert.equal(typeof zh[key], 'string', `Missing Chinese translation: ${key}`);
    assert.notEqual(en[key], '', `Empty English translation: ${key}`);
    assert.notEqual(zh[key], '', `Empty Chinese translation: ${key}`);
  }
});

const require = createRequire(import.meta.url);
const copy = JSON.parse(readFileSync(join(dir, '../../locales/en/plugins.json'), 'utf8'));
const translate = (key, values = {}) => (copy[key] ?? key).replace(/{{(\w+)}}/g, (_, name) => values[name] ?? '');

// Render real component branches; replace only external UI shells and data hooks.
function loadPluginComponent(file) {
  const source = ts.transpileModule(readFileSync(join(dir, file), 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
  }).outputText;
  const shell = ({ children, variant, className }) => React.createElement('div', { 'data-variant': variant, className }, children);
  const mocks = {
    'react-i18next': { useTranslation: () => ({ t: translate }) },
    '@tabler/icons-react': { IconAlertTriangle: () => null, IconArrowRight: () => null, IconPencil: () => null },
    '@/lib/utils': { cn: (...items) => items.filter(Boolean).join(' ') },
    './plugin-visuals': { PluginAvatar: () => null, PluginStatus: () => null },
    './plugin-config-form': { PluginConfigForm: () => React.createElement('button', null, 'CONFIG_FORM') },
  };
  const exports = {};
  new Function('require', 'exports', source)((name) => {
    if (name in mocks) return mocks[name];
    if (name.startsWith('@/components/ui/')) return new Proxy({}, { get: () => shell });
    return require(name);
  }, exports);
  return exports;
}

const { PluginGrantDialog } = loadPluginComponent('components/plugin-grant-dialog.tsx');
const { PluginDetailSheet } = loadPluginComponent('components/plugin-detail-sheet.tsx');
const renderGrant = (props) =>
  renderToStaticMarkup(
    React.createElement(PluginGrantDialog, {
      open: true,
      onOpenChange() {},
      mode: 'upgrade',
      pluginID: 'guard',
      pluginName: 'Guard',
      permissions: [],
      network: [],
      initialGranted: [],
      initialGrantedNetwork: [],
      onConfirm() {},
      ...props,
    })
  );
const basePlugin = {
  id: '1',
  pluginID: 'guard',
  name: 'Guard',
  version: '1',
  description: '',
  builtin: false,
  enabled: true,
  priority: 0,
  runtimeStatus: 'active',
  runtimeError: '',
  grantedPermissions: [],
  manifestPermissions: [],
  grantedNetwork: [],
  manifestNetwork: [],
  manifestHooks: {},
  manifestSchedules: [],
  requiredCapabilities: [],
  automationScope: { projects: [] },
  configFields: [],
  config: {},
};
const renderDetail = (plugin, props = {}) =>
  renderToStaticMarkup(
    React.createElement(PluginDetailSheet, {
      plugin: { ...basePlugin, ...plugin },
      canWrite: true,
      canManage: false,
      onEditPermissions() {},
      onOpenChange() {},
      ...props,
    })
  );

test('upgrade confirmation shows schedules and host capability requirements rather than no additions', () => {
  const html = renderGrant({
    preview: {
      currentVersion: '1',
      newVersion: '2',
      permissions: [],
      network: [],
      events: [],
      hooks: [],
      pages: [],
      actions: [],
      schedules: ['cache-check'],
      requiredCapabilities: ['usage_metrics.v1'],
    },
  });
  for (const text of ['New background schedules', 'cache-check', 'New required host capabilities', 'usage_metrics.v1']) {
    assert.ok(html.includes(text), text);
  }
  assert.ok(!html.includes(copy['plugins.grant.noAdditions']));
});

test('key suspension permission is high risk without a write prefix; metrics read access stays non-destructive', () => {
  const html = renderGrant({ mode: 'edit', permissions: ['manage_key_suspensions', 'read_usage_metrics'] });
  assert.ok(html.includes(copy['plugins.grant.keySuspensionWarning']));
  assert.ok(html.includes(copy['plugins.grant.usageMetricsHint']));
  assert.match(html, /text-destructive[^>]*>manage_key_suspensions/);
  assert.doesNotMatch(html, /text-destructive[^>]*>read_usage_metrics/);
  assert.ok(html.includes('High risk'));
});

test('detail exposes manifest schedules, capabilities and deny-by-default scope', () => {
  const html = renderDetail({
    manifestSchedules: [{ key: 'cache-check', intervalSeconds: 30 }],
    requiredCapabilities: ['usage_metrics.v1'],
  });
  for (const text of ['cache-check · every 30 seconds', 'usage_metrics.v1', copy['plugins.automation.scopeDenied']]) {
    assert.ok(html.includes(text), text);
  }
  const scoped = renderDetail({
    automationScope: {
      projects: [
        { projectId: 7, allKeys: true, apiKeyIds: [], excludedApiKeyIds: [8] },
        { projectId: 9, allKeys: false, apiKeyIds: [10], excludedApiKeyIds: [] },
        { projectId: 11, allKeys: false, apiKeyIds: [], excludedApiKeyIds: [] },
      ],
    },
  });
  for (const text of [
    'Project #7',
    'All keys in this project, except excluded keys',
    'Excluded key IDs: 8',
    'Approved key IDs: 10',
    'No keys approved in this project',
  ]) {
    assert.ok(scoped.includes(text), text);
  }
});

test('only granted key-suspending plugins lock configuration for non-owners', () => {
  const plugin = { grantedPermissions: ['manage_key_suspensions'] };
  const readonly = renderDetail(plugin);
  assert.ok(readonly.includes(copy['plugins.config.ownerOnly']));
  assert.ok(!readonly.includes('CONFIG_FORM'));
  assert.match(readonly, /data-variant="destructive"[^>]*>manage_key_suspensions/);
  assert.ok(renderDetail(plugin, { canManage: true }).includes('CONFIG_FORM'));
  assert.ok(renderDetail({ manifestPermissions: ['manage_key_suspensions'] }).includes('CONFIG_FORM'));
});

test('plugin query and every returning mutation carry automation fields and both install previews carry additions', () => {
  const text = readFileSync(join(dir, 'data/plugins.ts'), 'utf8');
  const ast = ts.createSourceFile('plugins.ts', text, ts.ScriptTarget.Latest, true);
  const declarations = new Map();
  for (const statement of ast.statements) {
    if (!ts.isVariableStatement(statement)) continue;
    for (const declaration of statement.declarationList.declarations)
      declarations.set(declaration.name.getText(ast), declaration.initializer?.getText(ast));
  }
  const fields = declarations.get('PLUGIN_FIELDS');
  for (const field of [
    'manifestSchedules',
    'key intervalSeconds',
    'requiredCapabilities',
    'automationScope',
    'projectId allKeys apiKeyIds excludedApiKeyIds',
  ]) {
    assert.ok(fields.includes(field), field);
  }
  for (const name of [
    'PLUGINS_QUERY',
    'INSTALL_PLUGIN_MUTATION',
    'INSTALL_PLUGIN_FROM_MARKET_MUTATION',
    'ENABLE_PLUGIN_MUTATION',
    'DISABLE_PLUGIN_MUTATION',
    'UPDATE_PLUGIN_SETTINGS_MUTATION',
  ]) {
    assert.ok(declarations.get(name).includes('${PLUGIN_FIELDS}'), name);
  }
  for (const name of ['INSTALL_PLUGIN_MUTATION', 'INSTALL_PLUGIN_FROM_MARKET_MUTATION']) {
    assert.match(declarations.get(name), /upgradePreview\s*{[^}]*\bschedules\b[^}]*\brequiredCapabilities\b/);
  }
});
