'use client';

import { useEffect, useState } from 'react';
import { Loader2, RefreshCw, Save } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { useRefreshProvidersCatalog } from '@/features/models/data/providers';
import { useCatalogSettings, useUpdateCatalogSettings } from '../data/system';

export function CatalogSettings() {
  const { t } = useTranslation();
  const { data, isLoading } = useCatalogSettings();
  const updateSettings = useUpdateCatalogSettings();
  const refreshCatalog = useRefreshProvidersCatalog();
  const [upstreamURL, setUpstreamURL] = useState('');
  const [refreshSeconds, setRefreshSeconds] = useState(3600);

  useEffect(() => {
    if (!data) {
      return;
    }

    setUpstreamURL(data.upstreamURL);
    setRefreshSeconds(data.refreshSeconds);
  }, [data]);

  if (isLoading) {
    return (
      <div className='flex h-24 items-center justify-center'>
        <Loader2 className='h-5 w-5 animate-spin' />
      </div>
    );
  }

  const handleSave = async () => {
    await updateSettings.mutateAsync({
      upstreamURL: upstreamURL.trim(),
      refreshSeconds,
    });
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>{t('system.catalog.title')}</CardTitle>
        <CardDescription>{t('system.catalog.description')}</CardDescription>
      </CardHeader>
      <CardContent className='space-y-6'>
        <div className='space-y-2'>
          <Label htmlFor='catalog-upstream-url'>{t('system.catalog.upstreamURL.label')}</Label>
          <Input
            id='catalog-upstream-url'
            value={upstreamURL}
            onChange={(event) => setUpstreamURL(event.target.value)}
            placeholder='https://raw.githubusercontent.com/ThinkInAIXYZ/PublicProviderConf/refs/heads/dev/dist/all.json'
          />
          <p className='text-muted-foreground text-sm'>{t('system.catalog.upstreamURL.description')}</p>
        </div>
        <div className='space-y-2'>
          <Label htmlFor='catalog-refresh-seconds'>{t('system.catalog.refreshSeconds.label')}</Label>
          <Input
            id='catalog-refresh-seconds'
            type='number'
            min={60}
            max={604800}
            value={refreshSeconds}
            onChange={(event) => setRefreshSeconds(Number(event.target.value) || 3600)}
          />
          <p className='text-muted-foreground text-sm'>{t('system.catalog.refreshSeconds.description')}</p>
        </div>
        <div className='flex flex-wrap gap-2'>
          <Button onClick={handleSave} disabled={updateSettings.isPending}>
            {updateSettings.isPending ? <Loader2 className='mr-2 h-4 w-4 animate-spin' /> : <Save className='mr-2 h-4 w-4' />}
            {t('system.buttons.save')}
          </Button>
          <Button variant='outline' onClick={() => refreshCatalog.mutate()} disabled={refreshCatalog.isPending}>
            {refreshCatalog.isPending ? (
              <Loader2 className='mr-2 h-4 w-4 animate-spin' />
            ) : (
              <RefreshCw className='mr-2 h-4 w-4' />
            )}
            {t('system.catalog.refreshNow')}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
